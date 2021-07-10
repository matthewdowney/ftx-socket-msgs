import time
import term
import os
import net.websocket
import json
import log

const (
	// If the difference between the time we see a message and the
	// messages reported timestamp eclipses this millisecond threshold,
	// consider the message stale.
	max_age       = 5000

	// Send {"op": "ping"} messages at regular intervals
	ping_interval = 15000

	// The path to log socket messages to
	path          = 'ftx.log'
)

// WS subscription message
struct Sub {
	op      string = 'subscribe'
	channel string
	market  string
}

fn sub_msg(channel string, market string) Sub {
	return {
		channel: channel
		market: market
	}
}

// Order book message structure
struct BookMessageFrame {
	channel  string
	market   string
	msg_type string          [json: @type]
	data     BookMessageData
}

struct BookMessageData {
	time     f64     [json: time]
	checksum i64
	bids     [][]f64
	asks     [][]f64
	action   string
}

// To allow JSON-serialization / logging of socket close data
struct SocketClose {
	code   int
	reason string
}

// Websocket handler extracts these two fields to send off to a monitoring
// process after logging the full message data to disk.
struct LatencyData {
	time       time.Time
	// time FTX reported for the message
	latency_ms f64
	// difference between that time and the time we saw
}

// Function which consumes LatencyData from the given channel in a loop,
// logging summary latency information to the console every `interval`ms.
fn monitor_latency(interval u64, ch chan LatencyData, mut logger log.Log) {
	now_milli := time.now().unix_time_milli()
	mut last_print := now_milli - (now_milli % interval)
	logger.info('Scheduling first message summary in ${interval - (now_milli - last_print)}ms')

	mut sum := f64(0.0)
	mut n := u64(0)
	mut n_stale := u64(0)
	mut alerted := false

	for {
		ld := <-ch ?

		// Check the latency for the message, determine if it is stale
		latency_s := ld.latency_ms / 1000.0
		is_stale := latency_s > max_age

		// Update the summary stats
		sum = sum + latency_s
		n = n + 1
		n_stale = if is_stale { n_stale + 1 } else { n_stale }

		// If this is the first stale message of the interval, print
		// an alert
		if is_stale && alerted {
			logger.warn('Message reported at $ld.time.format_ss_micro() is now ${term.red(ld.latency_ms.str())}ms old')
			alerted = true
		}

		// Finally, if the interval has elapsed, print a summary
		now := time.now().unix_time_milli()
		if now - last_print >= interval {
			// Summary
			if n > 0 {
				avg := sum / n
				stale_str := if alerted { term.red('$n_stale stale msgs') } else { 'no stale msgs' }
				avg_str := if avg < (max_age / 1000.0) {
					term.green('${avg:.3f}s')
				} else {
					term.red('${avg:.3f}s')
				}
				logger.info('Average latency of $avg_str across ${term.blue(n.str())} msgs ($stale_str)')
			} else {
				logger.info('No messages seen during interval')
			}

			// Reset variables
			sum = 0.0
			n = 0
			n_stale = 0
			alerted = false
			last_print = now
		}
	}
}

// Package the two references that the WS message handler needs: one to the
// latency data channel, another to the open log file.
struct WSHandlerRefs {
	markets []string
mut:
	ch   &chan LatencyData
	flog &os.File
}

fn ping_at_intervals(mut c websocket.Client, mut log os.File) ? {
	msg := '{"op": "ping"}'
	for {
		if c.state != websocket.State.open {
			break
		}
		c.write_string(msg) ?
		log.writeln('OUT $time.utc().format_ss_micro() $msg') ?
		time.sleep(ping_interval * time.millisecond)
	}
}

fn main() {
	// Read the markets to consume data from
	markets := os.args[1..].map(it.trim_space()).filter(it != '')
	if markets.len == 0 {
		println('Usage: ftx [markets]...')
		println('E.g. ftx BTC/USD BTC-PERP')
		exit(1)
	}

	// Set up a log file for saving all of the socket messages & a channel
	// to aggregate per-message latency data over, and package the
	// references together for the various WS handler callbacks (since
	// V has no closures)
	mut logf := os.open_append(path) or { panic("Couldn't open logfile ($path) for writing!") }
	lch := chan LatencyData{cap: 1024}
	mut refs := WSHandlerRefs{&markets, &lch, &logf}

	// Define a WS connection to FTX
	mut ws := websocket.new_client('wss://ftx.com/ws/') ?

	// Once open, subscribe to order book messages
	ws.on_open_ref(fn (mut c websocket.Client, mut refs WSHandlerRefs) ? {
		c.logger.info(term.green('Writing socket message log to ${path}...'))
		refs.flog.writeln('CONNECTED $time.utc().format_ss_micro()') ?
		c.logger.info('Subscribing to ${term.blue(refs.markets.len.str())} books...')

		for market in refs.markets {
			msg := sub_msg('orderbook', market)
			msg_str := json.encode(msg)
			c.write_string(msg_str) ?
			refs.flog.writeln('OUT $time.utc().format_ss_micro() $msg_str') ?
		}
	}, refs)

	// Upon receiving a message, get the current microtime before decoding
	// the frame, then check if the message has a timestamp / how old the
	// reported timestamp is compared to the current time. Then send the
	// latency data off to a channel for async processing.
	ws.on_message_ref(fn (mut c websocket.Client, msg &websocket.Message, mut refs WSHandlerRefs) ? {
		now := time.utc()

		// Decode the message frame
		payload := string(msg.payload)
		m := json.decode(BookMessageFrame, payload) ?

		msg_time_float := m.data.time
		if msg_time_float == 0 {
			// If there's no timestamp, just log the time and message
			// contents, with status "?" since we don't know how old the
			// message is supped to be.
			refs.flog.writeln('IN $now.format_ss_micro() OK ? $payload') ?
		} else {
			// Otherwise, convert the message timestamp field to
			// a time.Time
			msg_time_micros := i64(msg_time_float * 1000 * 1000)
			msg_time := time.unix2(int(msg_time_micros / 1000000), int(msg_time_micros % 1000000))

			// Look at the time elapsed between the reported time
			// and the local time
			latency := now - msg_time
			latency_ms := latency.milliseconds()

			// Log the message with status "OK" if the message is
			// newer than `max_age` seconds old, otherwise with status
			// "STALE"
			status := if latency_ms < max_age { 'OK' } else { 'STALE' }
			refs.flog.writeln('IN $now.format_ss_micro() $status $latency_ms $payload') ?

			// Send off the latency data
			ld := LatencyData{msg_time, f64(latency_ms)}
			ch := *refs.ch
			ch <- ld
		}
	}, refs)

	ws.on_error_ref(fn (mut ws websocket.Client, err string, mut log os.File) ? {
		log.writeln('ERROR $time.utc().format_ss_micro() $err') ?
	}, logf)

	ws.on_close_ref(fn (mut c websocket.Client, code int, reason string, mut log os.File) ? {
		sc := SocketClose{code, reason}
		log.writeln('DISCONNECTED $time.utc().format_ss_micro() ${json.encode(sc)}') ?
	}, logf)

	// Initiate the connection and start listening for messages in one
	// thread, handling latency data in another, and scheduling pings in
	// a third.
	logf.writeln('CONNECT $time.utc().format_ss_micro()') ?
	ws.connect() ?
	go ws.listen()
	go monitor_latency(30000, lch, mut ws.logger)
	go ping_at_intervals(mut ws, mut &logf)

	// Exit if the user enters a new line
	ws.logger.info(term.green('Started all processes. Enter a blank line to exit.'))
	_ := os.get_line()
	ws.logger.info('Saw blank line, shutting down...')

	ws.close(0, 'closed by client') or { println('Failed to close socket client') }
	refs.flog.writeln('EXIT $time.utc().format_ss_micro()') or { println('Failed to write exit') }
	refs.flog.close()

	exit(0)
}
