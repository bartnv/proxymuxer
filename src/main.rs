extern crate yaml_rust;
extern crate regex;

use std::fs::File;
use std::io::{Read, Write, ErrorKind};
use std::net::{TcpListener, TcpStream, Ipv6Addr};
use std::time::{Duration, Instant};
use std::thread;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, ATOMIC_USIZE_INIT};
use std::sync::atomic::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use yaml_rust::YamlLoader;
use regex::Regex;

#[derive(Clone)]
struct App {
  listenport: i64,
  startport: i64,
  sshargs: String
}

#[derive(Clone, Debug)]
struct Server {
    hostname: String,
    portno: i64,
    online: Arc<AtomicBool>
}

struct Matches {
  ipv4: Regex,
  ipv6: Regex,
  host1: Regex,
  host2: Regex
}

struct Rule {
  rule: String,
  regex: Regex,
  server: usize
}

static THREAD_COUNT: AtomicUsize = ATOMIC_USIZE_INIT;

fn main() {
  let mut app = App { listenport: 8080, startport: 61234, sshargs: String::from("-N") };
  let mut file = File::open("config.yml").unwrap();
  let mut config_str = String::new();
  file.read_to_string(&mut config_str).unwrap();
  let docs = YamlLoader::load_from_str(&config_str).unwrap();
  let config = &docs[0];
  let io_timeout = Duration::new(300, 0); // 5 minutes
  let re = Arc::new(Matches {
    ipv4: Regex::new(r"^(\d{1,3}\.\d{1,3})\.\d{1,3}\.\d{1,3}$").unwrap(),
    ipv6: Regex::new(r"^([0-9a-f]+:[0-9a-f]+:[0-9a-f]+):").unwrap(),
    host1: Regex::new(r"\.([^.]{4,}\.[a-z]+)$").unwrap(),
    host2: Regex::new(r"\.([^.]{4,}\.[a-z]+\.[a-z]+)$").unwrap()
  });

  if !config["listenport"].is_badvalue() {
      app.listenport = config["listenport"].as_i64().expect("Invalid 'listenport' setting in config.yml");
  }
  println!("Multiplexed proxy listening on port {}", app.listenport);
  if !config["startport"].is_badvalue() {
    app.startport = config["startport"].as_i64().expect("Invalid 'startport' setting in config.yml");
  }
  println!("Numbering loopback ports from {}", app.startport);
  if !config["sshargs"].is_badvalue() {
    app.sshargs = String::from(config["sshargs"].as_str().expect("Invalid 'sshargs' setting in config.yml"));
  }
  println!("Using ssh arguments: {}", app.sshargs);

  let mut servers: Vec<Server> = Vec::new();
  servers.push(Server { hostname: "direct".to_owned(), portno: 0, online: Arc::new(AtomicBool::new(true)) });

  for entry in config["servers"].as_vec().expect("Invalid 'servers' setting in config.yml") {
    let hostname = entry.as_str().expect("Invalid entry in 'servers' setting in config.yml").to_owned();
    let server = Server { hostname: hostname, portno: app.startport, online: Arc::new(AtomicBool::new(false)) };
    servers.push(server.clone());
    let argstring = app.sshargs.clone();
    app.startport += 1;
    println!("Found server {}", server.hostname);
    thread::spawn(move || {
      let recon_delay = Duration::new(60, 0);
      loop {
        println!("Connecting to {} with listen port {}", server.hostname, server.portno);
        let argvec: Vec<&str> = argstring.split_whitespace().collect();
        let mut child = Command::new("ssh")
                                .arg("-D")
                                .arg(format!("localhost:{}", server.portno))
                                .args(argvec)
                                .arg(server.hostname.clone())
                                .spawn().expect(&format!("Failed to launch ssh session to {}", server.hostname));
        server.online.store(true, Ordering::Relaxed);
        let ecode = child.wait().expect("Failed to wait on child");
        server.online.store(false, Ordering::Relaxed);
        if !ecode.success() {
            match ecode.code() {
                Some(code) => println!("Ssh session for {} failed with exit code {}", server.hostname, code),
                None => println!("Ssh session for {} was killed by a signal", server.hostname)
            }
        }
        println!("Waiting {} seconds before reconnecting...", recon_delay.as_secs());
        thread::sleep(recon_delay);
      }
    });
  }
  let servers = servers; // Make immutable

  let mut rules = Vec::new();
  for (rule, server) in config["rules"].as_hash().expect("Invalid 'rules' setting in config.yml") {
    let rule = rule.as_str().expect("Invalid key in 'rules' setting in config.yml").to_owned();
    let server = server.as_i64().expect("Invalid value in 'rules' setting in config.yml") as usize;
    let pattern = format!("(.*\\.)?{}", rule.replace(".", "\\."));
    rules.push(Rule { rule: rule, regex: Regex::new(&pattern).unwrap(), server: server });
    println!("Added rule {}", pattern);
  }
  let rules = Arc::new(rules); // RefCount and make immutable

  let server = TcpListener::bind(("0.0.0.0", app.listenport as u16)).unwrap();
  for client in server.incoming() {
    let mut stream = client.unwrap();
    stream.set_read_timeout(Some(io_timeout)).expect("Failed to set read timeout on TcpStream");
    stream.set_write_timeout(Some(io_timeout)).expect("Failed to set write timeout on TcpStream");
    let addr = stream.peer_addr().unwrap();
    let servers = servers.clone();
    let re = re.clone();
    let rules = rules.clone();
    thread::spawn(move || {
      let threads = THREAD_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
      let start = Instant::now();
      let mut req: [u8; 2048] = [0; 2048];
      match stream.read(&mut req) {
        Ok(c) => {
          if c == 0 {
            println!("CLOSE from {}", addr);
            return;
          }
          if req[0] == 5 {
            match stream.write(b"\x05\x00") {
              Ok(_) => {}
              Err(e) => {
                println!("WRITE ERROR to {}: {}", addr, e.to_string());
                return;
              }
            }
          }
          else {
            println!("Invalid auth request from {}", addr);
            let mut s = String::new();
            for &byte in req.iter() {
              s.push_str(&format!("{:X} ", byte));
            }
            println!("Debug: {}", s);
            return;
          }
        }
        Err(e) => {
          println!("READ ERROR from {}: {}", addr, e.to_string());
          return;
        }
      }
      match stream.read(&mut req) {
        Ok(c) => {
          let host;
          let mut port;

          if c == 0 {
            println!("CLOSE from {}", addr);
            return;
          }
          if &req[0..3] == b"\x05\x01\x00" {
            let mut idx = 0;
            let mut routing = "hash";
            if req[3] == b'\x01' { // IPv4
              host = format!("{}.{}", req[4], req[5]);
              port = req[8] as u16;
              port += req[9] as u16 >> 8;
              match select_server(&servers, &host) {
                Ok(i) => idx = i,
                Err(msg) => {
                  println!("{}", msg);
                  return;
                }
              }
            }
            else if req[3] == b'\x03' { // Hostname
              let length = req[4] as usize;
              host = String::from_utf8_lossy(&req[5..(5+length)]).into_owned();
              port = (req[5+length] as u16) << 8;
              port += req[5+length+1] as u16;
              for rule in rules.iter() {
                if rule.regex.is_match(&host) {
                  idx = rule.server;
                  routing = "rule";
                  break;
                }
              }
              if routing != "rule" {
                let hashhost =
                  if let Some(captures) = re.ipv4.captures(&host) { captures.get(1).unwrap().as_str() }
                  else if let Some(captures) = re.ipv6.captures(&host) { captures.get(1).unwrap().as_str() }
                  else if let Some(captures) = re.host1.captures(&host) { captures.get(1).unwrap().as_str() }
                  else if let Some(captures) = re.host2.captures(&host) { captures.get(1).unwrap().as_str() }
                  else { &host };
                match select_server(&servers, hashhost) {
                  Ok(i) => idx = i,
                  Err(msg) => {
                    println!("{}", msg);
                    return;
                  }
                }
              }
            }
            else if req[3] == b'\x04' { // IPv6
              let mut address: [u8; 16] = Default::default();
              address.copy_from_slice(&req[4..12]);
              host = format!("{}", Ipv6Addr::from(address));
              port = req[20] as u16;
              port += req[21] as u16 >> 8;
              match select_server(&servers, &host) {
                Ok(i) => idx = i,
                Err(msg) => {
                  println!("{}", msg);
                  return;
                }
              }
            }
            else {
              println!("Invalid connection request from {}", addr);
              return;
            }
            let server = servers.get(idx).expect("invalid server index");
            if server.online.load(Ordering::Relaxed) != true {
              println!("Selected server is offline");
              return;
            }
            println!("{:2} connections | [{}] Routed {}:{} to server {} ({})", threads, routing, host, port, idx, server.hostname);
            let mut tunnel = if idx == 0 {
              match TcpStream::connect((&*host, port)) {
                Ok(tunnel) => {
                  stream.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00").unwrap();
                  tunnel
                },
                Err(err) => {
                  match err.kind() {
                    ErrorKind::ConnectionRefused => { stream.write(b"\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00").unwrap(); },
                    _ => { stream.write(b"\x05\x01\x00\x01\x00\x00\x00\x00\x00\x00").unwrap(); }
                  }
                  return;
                }
              }
            }
            else { TcpStream::connect(("127.0.0.1", server.portno as u16)).expect("Failed to connect to tunnel port") };
            tunnel.set_read_timeout(Some(io_timeout)).expect("Failed to set read timeout on TcpStream");
            tunnel.set_write_timeout(Some(io_timeout)).expect("Failed to set write timeout on TcpStream");
            let mut buf: [u8; 1500] = [0; 1500];
            if idx != 0 {
              let _ = tunnel.write(b"\x05\x01\x00").unwrap();
              let _ = tunnel.read(&mut buf).unwrap(); // TODO: check the SOCKS5 response here
              let _ = tunnel.write(&req[0..c]).unwrap();
            }
            let mut tunnel_read = tunnel.try_clone().expect("Failed to clone tunnel TcpStream");
            let mut stream_write = stream.try_clone().expect("Failed to clone client TcpStream");
            let inbound = thread::spawn(move || {
              let mut buf: [u8; 1500] = [0; 1500];
              let mut count = 0;
              loop {
                match tunnel_read.read(&mut buf) {
                  Ok(c) => {
                    if c == 0 { return count; }
                    count += c;
                    if let Err(_) = stream_write.write_all(&buf[0..c]) {
                      println!("Write error on client");
                      return 0;
                    }
                  }
                  Err(_) => {
                    println!("Read error on tunnel");
                    return 0;
                  }
                }
              }
            });
            let mut count = 0;
            loop {
              match stream.read(&mut buf) {
                Ok(c) => {
                  if c == 0 { break; }
                  count += c;
                  if let Err(_) = tunnel.write_all(&buf[0..c]) {
                    println!("Write error on tunnel");
                    return;
                  }
                }
                Err(_) => {
                  println!("Read error on client");
                  return;
                }
              }
            }
            match inbound.join() {
              Ok(c) => {},//println!("Host {} port {} finished with {}b headers {}b data {}s duration", host, port, count, c, start.elapsed().as_secs()),
              Err(_) => println!("Host {} port {} reading thread panicked", host, port)
            }
          }
          else {
            println!("Invalid connection request from {}", addr);
            return;
          }
        }
        Err(e) => {
          println!("READ ERROR from {}: {}", addr, e.to_string());
          return;
        }
      }
      THREAD_COUNT.fetch_sub(1, Ordering::SeqCst);
    });
  }
}

fn select_server(servers: &Vec<Server>, host: &str) -> Result<usize, &'static str> {
  let mut hasher = DefaultHasher::new();
  host.hash(&mut hasher);
  let hash = hasher.finish() as usize;
  let serverno = (hash%(servers.len()-1))+1;
  let server = servers.get(serverno).unwrap();
  Ok(serverno)
}
