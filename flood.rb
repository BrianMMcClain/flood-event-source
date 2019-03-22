require 'httparty'
require 'optimist'
require 'json'
require 'logger'
require 'date'

$stdout.sync = true
@logger = Logger.new(STDOUT)
@logger.level = Logger::DEBUG

def poll_floods(lastTime, sink)
    url = "https://waterwatch.usgs.gov/webservices/flood?format=json&floodonly"
    response = HTTParty.get(url)
    j = JSON.parse(response.body)

    lastTimePoll = lastTime

    j["sites"].each do |site|
        # Filter out random null events
        if site["flow_dt"] != "0000-00-00 00:00:00"
            dt = parse_datetime(site["flow_dt"], site["tz_cd"])
            if dt > lastTime
                # New event!
                emit_flood(site, dt, sink)
                lastTimePoll = dt
            end
        end
    end

    return lastTimePoll
end

# Parse time, taking into consideration timezone offset
def parse_datetime(dt, tz)
    offset = "+00:00"
    case tz
    when "EST"
        offset = "−05:00"
    when "CST"
        offset = "−06:00"
    when "MST"
        offset = "−07:00"
    when "PST"
        offset = "−08:00"
    else
        offset = "+00:00"
    end

    parsedDate = DateTime.parse("#{dt}#{tz}")
    return parsedDate
end

# HTTP POST event data
def emit_flood(event, time, sink)
    data = {
        "time": time.to_s,
        "id": event["site_no"] + time.to_s.gsub(" ", ""),
        "type": "flood",
        "lat": event["dec_lat_va"],
        "long": event["dec_long_va"],
        "measure": (event["stage"] - event["floodstage"].to_f).round(2),
        "metadata": event.to_json
    }

    @logger.info("Sending message to #{sink}")
    r = HTTParty.post(sink, 
        :headers => {'Content-Type' => 'application/json'},
        :body => event.to_json
    )

    if r.code != 200
        @logger.error("Error! #{r}")
    end
end

# Parse CLI Flags
opts = Optimist::options do
    banner <<-EOS
Poll USGS flood data

Usage:
  ruby usgs-flood.rb

EOS
    opt :interval, "Poll Frequency", 
        :default => 10
    opt :sink, "Sink to send events", 
        :default => "http://localhost:8080"
end

# Arbitrary DateTime in the past, just as a starting space
lastTime = DateTime.parse("2019-01-01 09:30:00")
while true do
    @logger.debug("Polling . . .")
    lastTime = poll_floods(lastTime, opts[:sink])
    puts "New Last Time: #{lastTime}"
    sleep(opts[:interval])
end