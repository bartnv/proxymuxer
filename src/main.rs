extern crate yaml_rust;
extern crate regex;
extern crate prctl;

use std::fs::File;
use std::io::{Read, Write, ErrorKind, stdout};
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
    id: usize,
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
  let mut file = File::open("config.yml").expect("Failed to read configuration file: config.yml");
  let mut config_str = String::new();
  file.read_to_string(&mut config_str).expect("Configuration file contains invalid UTF8");
  let docs;
  match YamlLoader::load_from_str(&config_str) {
      Ok(r) => docs = r,
      Err(e) => {
          println!("Configuration file contains invalid YAML:");
          println!("{:?}", e);
          return;
      }
  }
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
  servers.push(Server { id: 0, hostname: "direct".to_owned(), portno: 0, online: Arc::new(AtomicBool::new(true)) });

  let mut count = 0;
  for entry in config["servers"].as_vec().expect("Invalid 'servers' setting in config.yml") {
    count += 1;
    let hostname = entry.as_str().expect("Invalid entry in 'servers' setting in config.yml").to_owned();
    let server = Server { id: count, hostname: hostname, portno: app.startport, online: Arc::new(AtomicBool::new(false)) };
    servers.push(server.clone());
    let argstring = app.sshargs.clone();
    app.startport += 1;
    println!("Added server {}: {}", count, server.hostname);
    thread::spawn(move || {
      prctl::set_name(&format!("Server {}", server.id)).expect("Failed to set process name");
      let recon_delay = Duration::new(60, 0);
      thread::sleep(Duration::new(1, 0));
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

  let pool = match config["pool"].as_vec() {
    Some(array) => {
      let mut v = Vec::new();
      for entry in array { v.push(entry.as_i64().unwrap() as usize); }
      v
    }
    None => {
      let mut v = Vec::new();
      for server in &servers { v.push(server.id); };
      v
    }
  };
  println!("Set hash-based random pool to {:?}", pool);

  let mut rules = Vec::new();
  for (rule, server) in config["rules"].as_hash().expect("Invalid 'rules' setting in config.yml") {
    let rule = rule.as_str().expect("Invalid key in 'rules' setting in config.yml").to_owned();
    let server = server.as_i64().expect("Invalid value in 'rules' setting in config.yml") as usize;
    let pattern = format!("(.*\\.)?{}", rule.replace(".", "\\."));
    println!("Added rule {} for server {}", &rule, &server);
    rules.push(Rule { rule: rule, regex: Regex::new(&pattern).unwrap(), server: server });
  }
  let rules = Arc::new(rules); // RefCount and make immutable

  let server = TcpListener::bind(("0.0.0.0", app.listenport as u16)).expect("Failed to bind to listen port");
  for client in server.incoming() {
    let mut stream = match client {
      Ok(stream) => stream,
      Err(err) => {
        println!("Failed to accept connection: {:?}", err);
        continue;
      }
    };
    stream.set_read_timeout(Some(io_timeout)).expect("Failed to set read timeout on TcpStream");
    stream.set_write_timeout(Some(io_timeout)).expect("Failed to set write timeout on TcpStream");
    let addr = match stream.peer_addr() {
      Ok(addr) => addr,
      Err(e) => {
        println!("Failed to get peer_addr() from stream: {}", e.to_string());
        continue;
      }
    };
    let servers = servers.clone();
    let pool = pool.clone();
    let re = re.clone();
    let rules = rules.clone();
    thread::spawn(move || {
      let threads = THREAD_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
      let _start = Instant::now();
      let proto;
      let mut bytes = 0;
      let mut req: [u8; 2048] = [0; 2048];
      match stream.read(&mut req) {
        Ok(c) => {
          if c == 0 {
            println!("Incoming connection from {} closed before protocol exchange", addr);
            cleanup();
            return;
          }
          if req[0] == 5 { // SOCKS5
            proto = 5;
            match stream.write(b"\x05\x00") {
              Ok(_) => {}
              Err(e) => {
                println!("Incoming connection from {} lost during protocol exchange: {}", addr, e.to_string());
                cleanup();
                return;
              }
            }
          }
          else if req[0] == 4 { // SOCKS4
            proto = 4;
            bytes = c;
          }
          else {
            println!("Invalid auth request from {}", addr);
            cleanup();
            return;
          }
        }
        Err(e) => {
          println!("Incoming connection from {} lost before protocol exchange: {}", addr, e.to_string());
          cleanup();
          return;
        }
      }
      if proto == 5 {
        match stream.read(&mut req) {
          Ok(c) => match c {
            0 => {
              println!("Incoming connection from {} closed after protocol exchange", addr);
              cleanup();
              return;
            },
            _ => bytes = c
          }
          Err(e) => {
            println!("Incoming connection from {} lost after protocol exchange: {}", addr, e.to_string());
            cleanup();
            return;
          }
        }
      }

      let host;
      let mut port;
      let mut idx = 0;
      let mut routing = "hash";

      if proto == 5 {
        if &req[0..3] != b"\x05\x01\x00" {
          println!("Invalid SOCKS5 request from {}", addr);
          cleanup();
          return;
        }
        if req[3] == b'\x01' { // IPv4
          host = format!("{}.{}.{}.{}", req[4], req[5], req[6], req[7]);
          port = (req[8] as u16) << 8;
          port += req[9] as u16;
        }
        else if req[3] == b'\x03' { // Hostname
          let length = req[4] as usize;
          host = String::from_utf8_lossy(&req[5..(5+length)]).into_owned();
          port = (req[5+length] as u16) << 8;
          port += req[5+length+1] as u16;
        }
        else if req[3] == b'\x04' { // IPv6
          let mut address: [u8; 16] = Default::default();
          address.copy_from_slice(&req[4..20]);
          host = format!("{}", Ipv6Addr::from(address));
          port = (req[20] as u16) << 8;
          port += req[21] as u16;
        }
        else {
          println!("Invalid SOCKS5 address request from {}", addr);
          cleanup();
          return;
        }
      }
      else {
        if &req[0..2] != b"\x04\x01" {
          println!("Invalid SOCKS4 request from {}", addr);
          cleanup();
          return;
        }
        host = format!("{}.{}.{}.{}", req[4], req[5], req[6], req[7]);
        port = (req[2] as u16) << 8;
        port += req[3] as u16;
      }

      for rule in rules.iter() {
        if rule.regex.is_match(&host) {
          idx = rule.server;
          routing = "rule";
          prctl::set_name(&format!("Rule {}", rule.rule)).expect("Failed to set process name");
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
        prctl::set_name(&format!("Hash {}", hashhost)).expect("Failed to set process name");
        match select_server(&pool, hashhost) {
          Ok(i) => idx = i,
          Err(msg) => {
            println!("{}", msg);
            cleanup();
            return;
          }
        }
      }
      let server = servers.get(idx).expect("Invalid server index");
      if server.online.load(Ordering::Relaxed) != true {
        println!("Selected server is offline");
        cleanup();
        return;
      }
      println!("\r{:3} connections | [{}] Routed {}:{} to server {} ({})", threads, routing, host, port, idx, server.hostname);

      let mut tunnel = if idx == 0 {
        match TcpStream::connect((&*host, port)) {
          Ok(tunnel) => {
            if proto == 4 { stream.write(b"\x00\x5A").expect("Failed to write SOCKS4 handshake to tunnel"); }
            else { stream.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00").expect("Failed to write SOCKS5 handshake to tunnel"); }
            tunnel
          },
          Err(err) => {
            println!("Failed to make direct connection to {}:{}", host, port);
            if proto == 4 { stream.write(b"\x00\x5B").expect("Failed to write SOCKS4 error back to client"); }
            else {
              match err.kind() {
                ErrorKind::ConnectionRefused => { stream.write(b"\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00").expect("Failed to write SOCKS5 error back to client"); },
                _ => { stream.write(b"\x05\x01\x00\x01\x00\x00\x00\x00\x00\x00").expect("Failed to write SOCKS5 error back to client"); }
              }
            }
            cleanup();
            return;
          }
        }
      }
      else { TcpStream::connect(("127.0.0.1", server.portno as u16)).expect("Failed to connect to tunnel port") }; // TODO: report error back to client here too
      tunnel.set_read_timeout(Some(io_timeout)).expect("Failed to set read timeout on TcpStream");
      tunnel.set_write_timeout(Some(io_timeout)).expect("Failed to set write timeout on TcpStream");
      let mut buf: [u8; 1500] = [0; 1500];
      if idx != 0 {
        if proto == 4 { tunnel.write(&req[0..bytes]).expect("Failed to write SOCKS4 request to tunnel"); }
        else {
          let _ = tunnel.write(b"\x05\x01\x00").expect("Failed to write SOCKS5 request to tunnel");
          let _ = tunnel.read(&mut buf).expect("Failed to write SOCKS5 request to tunnel"); // TODO: check the SOCKS5 response here
          let _ = tunnel.write(&req[0..bytes]).expect("Failed to write SOCKS5 request to tunnel");
        }
      }
      let mut tunnel_read = tunnel.try_clone().expect("Failed to clone tunnel TcpStream");
      let mut stream_write = stream.try_clone().expect("Failed to clone client TcpStream");
      let thr_host = host.clone();
      let thr_serv = server.hostname.clone();

      let inbound = thread::spawn(move || {
        let mut buf: [u8; 1500] = [0; 1500];
        let mut count = 0;
        prctl::set_name("Reader").expect("Failed to set process name");
        loop {
          match tunnel_read.read(&mut buf) {
            Ok(c) => {
              if c == 0 { return count; }
              count += c;
              if let Err(e) = stream_write.write_all(&buf[0..c]) {
                println!("\r[{}/{}] Write error on client: {}", thr_serv, thr_host, e.to_string());
                return count;
              }
            }
            Err(e) => {
              if e.kind() == ErrorKind::WouldBlock { println!("\r[{}/{}] Read timeout on tunnel", thr_serv, thr_host); }
              else { println!("\r[{}/{}] Read error on tunnel: {}", thr_serv, thr_host, e.to_string()); }
              let _ = stream_write.shutdown(std::net::Shutdown::Both);
              return count;
            }
          }
        }
      });
      let mut _outbound = 0;
      loop {
        match stream.read(&mut buf) {
          Ok(c) => {
            if c == 0 { break; }
            _outbound += c;
            // println!("\rHost {} port {} outbound {} duration {}s", host, port, _outbound, _start.elapsed().as_secs());
            if let Err(e) = tunnel.write_all(&buf[0..c]) {
              println!("\r[{}/{}] Write error on tunnel: {}", server.hostname, host, e.to_string());
              break;
            }
          }
          Err(e) => {
            if e.kind() == ErrorKind::WouldBlock { println!("\r[{}/{}] Read timeout on client", server.hostname, host); }
            else { println!("\r[{}/{}] Read error on client: {}", server.hostname, host, e.to_string()); }
            let _ = tunnel.shutdown(std::net::Shutdown::Both);
            break;
          }
        }
      }
      match inbound.join() {
        Ok(_) => {},//println!("\rHost {} port {} finished with {}b headers {}b data {}s duration", host, port, _outbound, c, _start.elapsed().as_secs()),
        Err(_) => println!("\rHost {} port {} reading thread panicked", host, port)
      }
      cleanup();
    });
  }
}

fn select_server(pool: &Vec<usize>, host: &str) -> Result<usize, &'static str> {
  let mut hasher = DefaultHasher::new();
  host.hash(&mut hasher);
  let hash = hasher.finish() as usize;
  let serverno = pool[hash%pool.len()];
  Ok(serverno)
}

fn cleanup() {
  let threads = THREAD_COUNT.fetch_sub(1, Ordering::SeqCst);
  print!("\r{:3} connections | ", threads-1);
  stdout().flush().expect("Failed to flush stdout");
}
