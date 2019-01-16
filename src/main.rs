extern crate yaml_rust;
extern crate regex;
extern crate prctl;

use std::fs::{File, OpenOptions};
use std::io::{Read, Write, ErrorKind, stdout};
use std::net::{TcpListener, TcpStream, SocketAddr, Ipv6Addr};
use std::time::{Duration, Instant};
use std::thread;
use std::process::Command;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{Ordering, AtomicBool, AtomicUsize, ATOMIC_USIZE_INIT};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use yaml_rust::YamlLoader;
use regex::Regex;

#[derive(Clone)]
struct App {
  listenport: i64,
  startport: i64,
  sshargs: String,
  connlog: String,
  conntimeout: Duration,
  idletimeout: Duration
}

#[derive(Clone, Debug)]
struct Server {
    id: usize,
    hostname: String,
    portno: i64,
    online: Arc<AtomicBool>,
    online_since: Arc<RwLock<Instant>>,
    responsive: Arc<AtomicBool>,
    conn_count: Arc<AtomicUsize>,
    conn_avg: Arc<AtomicUsize>,
    conn_best: Arc<AtomicUsize>,
    errors: Arc<RwLock<Vec<bool>>>
}
impl Server {
  fn new(id: usize, hostname: String, portno: i64) -> Server {
    Server {
      id, hostname, portno,
      online: Arc::new(AtomicBool::new(false)),
      online_since: Arc::new(RwLock::new(Instant::now())),
      responsive: Arc::new(AtomicBool::new(true)),
      conn_count: Arc::new(ATOMIC_USIZE_INIT),
      conn_avg: Arc::new(ATOMIC_USIZE_INIT),
      conn_best: Arc::new(AtomicUsize::new(usize::max_value())),
      errors: Arc::new(RwLock::new(vec![false; 10]))
    }
  }
  fn push_status(&self, is_error: bool) {
    let mut errors = self.errors.write().unwrap();
    errors.remove(0);
    errors.push(is_error);
  }
}

#[derive(Clone, Debug)]
struct Connection {
  peer_addr: SocketAddr,
  start: Instant,
  proto: u16,
  hostname: String,
  portno: u16,
  outbound: u64,
  inbound: usize,
  conn_ms: usize,
  data_ms: usize,
  errors: String
}
impl Connection {
  fn new(peer_addr: SocketAddr) -> Connection {
    Connection { peer_addr, start: Instant::now(), proto: 0, hostname: String::new(), portno: 0, outbound: 0, inbound: 0, conn_ms: 0, data_ms: 0, errors: String::new() }
  }
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
  let mut app = App {
    listenport: 8080,
    startport: 61234,
    sshargs: String::from("-N"),
    connlog: String::new(),
    conntimeout: Duration::new(10, 0), // 10 seconds
    idletimeout: Duration::new(300, 0) // 5 minutes
  };
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
  if !config["connectionlog"].is_badvalue() {
    app.connlog = config["connectionlog"].as_str().expect("Invalid 'connectionlog' setting in config.yml").to_string();
    if !app.connlog.is_empty() { println!("Writing connection log to {}", app.connlog); }
  }
  if !config["conntimeout"].is_badvalue() {
    let seconds = config["conntimeout"].as_i64().expect("Invalid 'conntimeout' setting in config.yml");
    if seconds <= 0 { panic!("Invalid 'conntimeout' setting in config.yml: must be larger than zero") }
    app.conntimeout = Duration::new(seconds as u64, 0);
    println!("Connection timeout set to {} seconds", seconds);
  }
  if !config["idletimeout"].is_badvalue() {
    let seconds = config["idletimeout"].as_i64().expect("Invalid 'idletimeout' setting in config.yml");
    if seconds <= 0 { panic!("Invalid 'idletimeout' setting in config.yml: must be larger than zero") }
    app.idletimeout = Duration::new(seconds as u64, 0);
    println!("Idle timeout set to {} seconds", seconds);
  }

  let mut servers: Vec<Server> = Vec::new();
  {
    let direct = Server::new(0, "direct".to_owned(), 0);
    direct.online.store(true, Ordering::Relaxed);
    servers.push(direct);
  }

  let mut count = 0;
  for entry in config["servers"].as_vec().expect("Invalid 'servers' setting in config.yml") {
    count += 1;
    let hostname = entry.as_str().expect("Invalid entry in 'servers' setting in config.yml").to_owned();
    let server = Server::new(count, hostname, app.startport);
    servers.push(server.clone());
    let argstring = app.sshargs.clone();
    app.startport += 1;
    println!("Added server {}: {}", count, server.hostname);
    thread::spawn(move || {
      prctl::set_name(&format!("Server {}", server.id)).expect("Failed to set process name");
      let recon_delay = Duration::new(60, 0);
      thread::sleep(Duration::new(1, 0));
      loop {
        println!("\rConnecting to {} with listen port {}", server.hostname, server.portno);
        let argvec: Vec<&str> = argstring.split_whitespace().collect();
        let mut child = Command::new("ssh")
                                .arg("-D")
                                .arg(format!("localhost:{}", server.portno))
                                .args(argvec)
                                .arg(server.hostname.clone())
                                .spawn().unwrap_or_else(|_| panic!("Failed to launch ssh session to {}", server.hostname));
        server.online.store(true, Ordering::Relaxed);
        {
          let mut instant = server.online_since.write().unwrap();
          *instant = Instant::now();
        }
        let ecode = child.wait().expect("Failed to wait on child");
        server.online.store(false, Ordering::Relaxed);
        if !ecode.success() {
            match ecode.code() {
                Some(code) => println!("\rSsh session for {} failed with exit code {}", server.hostname, code),
                None => println!("\rSsh session for {} was killed by a signal", server.hostname)
            }
        }
        println!("\rWaiting {} seconds before reconnecting...", recon_delay.as_secs());
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
    rules.push(Rule { rule, regex: Regex::new(&pattern).unwrap(), server });
  }
  let rules = Arc::new(rules); // RefCount and make immutable

  let server = TcpListener::bind(("0.0.0.0", app.listenport as u16)).expect("Failed to bind to listen port");
  let mut info_last = Instant::now();
  for client in server.incoming() {
    let mut stream = match client {
      Ok(stream) => stream,
      Err(err) => {
        println!("\rFailed to accept connection: {:?}", err);
        continue;
      }
    };
    stream.set_read_timeout(Some(app.conntimeout)).expect("Failed to set read timeout on TcpStream");
    stream.set_write_timeout(Some(app.conntimeout)).expect("Failed to set write timeout on TcpStream");
    if stream.peer_addr().is_err() {
      println!("\rFailed to get peer_addr() from stream: {}", stream.peer_addr().unwrap_err().to_string());
      continue;
    }
    let mut connection = Connection::new(stream.peer_addr().unwrap());
    let thr_servers = servers.clone();
    let pool = pool.clone();
    let re = re.clone();
    let rules = rules.clone();
    let app = app.clone();

    thread::spawn(move || { // connection thread
      let threads = THREAD_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
      let mut bytes = 0;
      let mut req: [u8; 2048] = [0; 2048];
      match stream.read(&mut req) {
        Ok(c) => {
          if c == 0 { println!("\rIncoming connection from {} closed before protocol exchange", connection.peer_addr); }
          else if req[0] == 5 { // SOCKS5
            match stream.write(b"\x05\x00") {
              Ok(2) => { connection.proto = 5; },
              Err(e) => { println!("\rIncoming connection from {} lost during protocol exchange: {}", connection.peer_addr, e.to_string()); },
              _ => { println!("\rIncoming connection from {} lost during protocol exchange", connection.peer_addr); }
            }
          }
          else if req[0] == 4 { // SOCKS4
            connection.proto = 4;
            bytes = c;
          }
          else {
            println!("\rInvalid request from {}: {}", connection.peer_addr, String::from_utf8_lossy(&req).lines().next().unwrap_or("<binary>"));
            thread::sleep(Duration::new(5, 0)); // Some devices retry immediately, so throttle a little here
          }
        }
        Err(e) => { println!("\rIncoming connection from {} lost before protocol exchange: {}", connection.peer_addr, e.to_string()); }
      }
      if connection.proto == 0 { cleanup(""); return; } // Protocol negotiation failed

      if connection.proto == 5 {
        match stream.read(&mut req) {
          Ok(c) if c > 0 => { bytes = c },
          _ => {
            cleanup(&format!("Incoming connection from {} closed after protocol exchange", connection.peer_addr));
            return;
          }
        }
      }

      let mut idx = 0;
      let mut routing = "hash";

      if connection.proto == 5 {
        if &req[0..3] != b"\x05\x01\x00" {
          cleanup(&format!("Invalid SOCKS5 request from {}", connection.peer_addr));
          return;
        }
        if req[3] == b'\x01' { // IPv4
          connection.hostname = format!("{}.{}.{}.{}", req[4], req[5], req[6], req[7]);
          connection.portno = u16::from(req[8]) << 8;
          connection.portno += u16::from(req[9]);
        }
        else if req[3] == b'\x03' { // Hostname
          let length = req[4] as usize;
          connection.hostname = String::from_utf8_lossy(&req[5..(5+length)]).into_owned();
          connection.portno = u16::from(req[5+length]) << 8;
          connection.portno += u16::from(req[5+length+1]);
        }
        else if req[3] == b'\x04' { // IPv6
          let mut address: [u8; 16] = Default::default();
          address.copy_from_slice(&req[4..20]);
          connection.hostname = format!("{}", Ipv6Addr::from(address));
          connection.portno = u16::from(req[20]) << 8;
          connection.portno += u16::from(req[21]);
        }
        else {
          cleanup(&format!("Invalid SOCKS5 address request from {}", connection.peer_addr));
          return;
        }
      }
      else {
        if &req[0..2] != b"\x04\x01" {
          cleanup(&format!("Invalid SOCKS4 request from {}", connection.peer_addr));
          return;
        }
        connection.hostname = format!("{}.{}.{}.{}", req[4], req[5], req[6], req[7]);
        connection.portno = u16::from(req[2]) << 8;
        connection.portno += u16::from(req[3]);
      }

      for rule in rules.iter() {
        if rule.regex.is_match(&connection.hostname) {
          idx = rule.server;
          routing = "rule";
          prctl::set_name(&format!("Rule {}", rule.rule)).expect("Failed to set process name");
          break;
        }
      }
      if routing != "rule" {
        let hashhost =
          if let Some(captures) = re.ipv4.captures(&connection.hostname) { captures.get(1).unwrap().as_str() }
          else if let Some(captures) = re.ipv6.captures(&connection.hostname) { captures.get(1).unwrap().as_str() }
          else if let Some(captures) = re.host1.captures(&connection.hostname) { captures.get(1).unwrap().as_str() }
          else if let Some(captures) = re.host2.captures(&connection.hostname) { captures.get(1).unwrap().as_str() }
          else { &connection.hostname };
        prctl::set_name(&format!("Hash {}", hashhost)).expect("Failed to set process name");
        match select_server(&pool, hashhost) {
          Ok(i) => idx = i,
          Err(msg) => {
            cleanup(msg);
            return;
          }
        }
      }
      let mut server = thr_servers.get(idx).expect("Invalid server index");
      if server.online.load(Ordering::Relaxed) != true {
        if routing == "rule" {
          cleanup("Rule directed server is offline");
          return;
        }
        let mut i = 0;
        let result = loop {
          if i == pool.len() { break None; }
          let server = thr_servers.get(pool[i]).expect("Invalid server index in pool");
          if server.online.load(Ordering::Relaxed) { break Some(server); }
          i += 1;
        };
        match result {
          Some(res) => { server = res; }
          None => {
            cleanup("No online server found in pool");
            return;
          }
        }
      }

      {
        let errors = server.errors.read().unwrap();
        let mut count = 0;
        for error in errors.iter() {
          if *error { count += 1; }
        }
        if count > 0 { println!("\rServer {} has {}% error rate", idx, count*10); }
      }

      let server = server;
      println!("\r{:3} connections | [{}] Routed {}:{} to server {} ({})", threads, routing, connection.hostname, connection.portno, idx, server.hostname);

      let mut tunnel = if idx == 0 { // Handle server 0 (direct connection) case
        match TcpStream::connect((&*connection.hostname, connection.portno)) {
          Ok(tunnel) => {
            if connection.proto == 4 {
              match stream.write(b"\x00\x5A") {
                Ok(2) => (),
                _ => {
                  cleanup("Failed to write SOCKS4 handshake to tunnel");
                  return;
                }
              }
            }
            else {
              match stream.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00") {
                Ok(10) => (),
                _ => {
                  cleanup("Failed to write SOCKS5 handshake to tunnel");
                  return;
                }
              }
            }
            tunnel
          },
          Err(err) => {
            let mut error = format!("\rFailed to make direct connection to {}:{}", connection.hostname, connection.portno);
            if connection.proto == 4 {
              match stream.write(b"\x00\x5B") {
                Ok(2) => (),
                _ => error.push_str("Failed to write SOCKS4 error back to client")
              }
            }
            else {
              match err.kind() {
                ErrorKind::ConnectionRefused => {
                  match stream.write(b"\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00") {
                    Ok(10) => (),
                    _ => error.push_str("Failed to write SOCKS5 error back to client")
                  }
                },
                _ => {
                  match stream.write(b"\x05\x01\x00\x01\x00\x00\x00\x00\x00\x00") {
                    Ok(10) => (),
                    _ => error.push_str("Failed to write SOCKS5 error back to client")
                  }
                }
              }
            }
            cleanup(&error);
            return;
          }
        }
      }
      else {
        match TcpStream::connect(("127.0.0.1", server.portno as u16)) {
          Ok(stream) => stream,
          Err(_) => {
            server.push_status(true);
            cleanup("Failed to connect to tunnel port");
            return;
          }
        }
      };

      tunnel.set_read_timeout(Some(app.conntimeout)).expect("Failed to set read timeout on TcpStream");
      tunnel.set_write_timeout(Some(app.conntimeout)).expect("Failed to set write timeout on TcpStream");
      let mut buf: [u8; 1500] = [0; 1500];
      if idx != 0 {
        if connection.proto == 4 {
          match tunnel.write(&req[0..bytes]) {
            Ok(c) if c == bytes => (),
            _ => {
              server.push_status(true);
              cleanup("Failed to write SOCKS4 request to tunnel");
              return;
            }
          }
        }
        else {
          match tunnel.write(b"\x05\x01\x00") {
            Ok(3) => (),
            _ => {
              server.push_status(true);
              cleanup("Failed to write SOCKS5 request to tunnel");
              return;
            }
          }
          match tunnel.read(&mut buf) { // TODO: check the SOCKS5 response here
            Ok(_) => (),
            _ => {
              server.push_status(true);
              cleanup("Failed to read SOCKS5 auth response from tunnel");
              return;
            }
          }
          match tunnel.write(&req[0..bytes]) {
            Ok(c) if c == bytes => (),
            _ => {
              server.push_status(true);
              cleanup("Failed to write SOCKS5 request to tunnel");
              return;
            }
          }
        }
      }
      let mut tunnel_read = tunnel.try_clone().expect("Failed to clone tunnel TcpStream");
      let mut stream_write = stream.try_clone().expect("Failed to clone client TcpStream");
      let thr_conn = connection.clone();
      let thr_serv = server.clone();
      let thr_app = app.clone();

      let inbound = thread::spawn(move || { // inbound data thread
        let mut buf: [u8; 1500] = [0; 1500];
        let mut count = 0;
        let mut bytes = 0;
        let mut conn_ms = 0;
        let mut data_ms = 0;
        prctl::set_name("Reader").expect("Failed to set process name");
        loop {
          match tunnel_read.read(&mut buf) {
            Ok(c) => {
              if c == 0 {
                let _ = stream_write.shutdown(std::net::Shutdown::Both);
                return (bytes, conn_ms, data_ms, "");
              }
              count += 1;
              if count == 1 { // SOCKS server response
                tunnel_read.set_read_timeout(Some(thr_app.idletimeout)).expect("Failed to set read timeout on TcpStream");
                conn_ms = (thr_conn.start.elapsed().as_secs()*1000) as usize; // TODO: change this to use Duration::as_millis() as soon as that feature stabilizes
                conn_ms += thr_conn.start.elapsed().subsec_millis() as usize;
                thr_serv.conn_count.fetch_add(1, Ordering::Relaxed);
                thr_serv.conn_avg.fetch_add(conn_ms, Ordering::Relaxed);
                // TODO: use AtomicUsize::fetch_min().min() here once the feature stabilizes
                if conn_ms < thr_serv.conn_best.load(Ordering::Relaxed) { thr_serv.conn_best.store(conn_ms, Ordering::Relaxed); }
                if buf[0] == 5 && buf[1] != 0 {
                  println!("\rServer {} returned SOCKS5 status code {:02X}", thr_serv.hostname, buf[1]);
                  return (0, 0, 0, " / server returned SOCKS5 error code");
                }
              }
              else if count == 2 { // First data from remote host
                data_ms = (thr_conn.start.elapsed().as_secs()*1000) as usize; // TODO: change this to use Duration::as_millis() as soon as that feature stabilizes
                data_ms += thr_conn.start.elapsed().subsec_millis() as usize;
              }
              if count != 1 { bytes += c; } // Don't count the SOCKS protocol response as payload bytes
              if let Err(e) = stream_write.write_all(&buf[0..c]) {
                println!("\r[{}/{}] Write error on client: {}", thr_serv.hostname, thr_conn.hostname, e.to_string());
                return (bytes, conn_ms, data_ms, " / write error on client");
              }
            }
            Err(e) => {
              let _ = stream_write.shutdown(std::net::Shutdown::Both);
              if e.kind() == ErrorKind::WouldBlock {
                println!("\r[{}/{}] Read timeout on tunnel ({} bytes read)", thr_serv.hostname, thr_conn.hostname, bytes);
                return (bytes, conn_ms, data_ms, " / read timeout on tunnel");
              }
              else {
                println!("\r[{}/{}] Read error on tunnel: {}", thr_serv.hostname, thr_conn.hostname, e.to_string());
                return (bytes, conn_ms, data_ms, " / read error on tunnel");
              }
            }
          }
        }
      });

      loop {
        match stream.read(&mut buf) {
          Ok(c) => {
            if c == 0 {
              if connection.outbound == 0 { connection.errors.push_str(" / no requests"); }
              break;
            }
            if connection.outbound == 0 { stream.set_read_timeout(Some(app.idletimeout)).expect("Failed to set read timeout on TcpStream"); }
            connection.outbound += c as u64;
            if let Err(e) = tunnel.write_all(&buf[0..c]) {
              println!("\r[{}/{}] Write error on tunnel: {}", server.hostname, connection.hostname, e.to_string());
              connection.errors.push_str(" / write error on tunnel");
              break;
            }
          }
          Err(e) => {
            if e.kind() == ErrorKind::WouldBlock {
              if connection.outbound > 0 {
                println!("\r[{}/{}] Read timeout on client ({} bytes read)", server.hostname, connection.hostname, connection.outbound);
                connection.errors.push_str(" / read timeout on client");
              }
              else { connection.errors.push_str(" / no requests"); }
            }
            else {
              println!("\r[{}/{}] Read error on client: {}", server.hostname, connection.hostname, e.to_string());
              connection.errors.push_str(" / read error on client");
              let _ = tunnel.shutdown(std::net::Shutdown::Both);
            }
            break;
          }
        }
      }
      match inbound.join() {
        Ok((inbound, conn_ms, data_ms, errors)) => {
          connection.inbound = inbound;
          connection.conn_ms = conn_ms;
          connection.data_ms = data_ms;
          if connection.errors != " / no requests" { connection.errors.push_str(errors); }
        },
        Err(_) => {
          println!("\rHost {} port {} reading thread panicked", connection.hostname, connection.portno);
          connection.errors.push_str(" / reading thread panicked");
        }
      }

      server.push_status(connection.errors.contains("tunnel"));
      cleanup("");

      if !app.connlog.is_empty() {
        let mut file = OpenOptions::new().append(true).create(true).open(&app.connlog).expect("Failed to open connection log file");
        let mut line = Vec::new();
        writeln!(line, "Connection to {}:{} {}-routed through {} finished after {}s with {}b outbound, {}b inbound / timings: {}ms connect {}ms first data{}", connection.hostname, connection.portno, routing, server.hostname, connection.start.elapsed().as_secs(), connection.outbound, connection.inbound, connection.conn_ms, connection.data_ms, connection.errors).unwrap();
        file.write_all(&line).expect("\rFailed to write to connection log");
      }
    });

    if info_last.elapsed() > Duration::new(900, 0) {
      for server in servers.clone() {
        let con_avg = match server.conn_count.load(Ordering::Relaxed) {
          0 => 0,
          _ => server.conn_avg.load(Ordering::Relaxed)/server.conn_count.load(Ordering::Relaxed)
        };
        println!("\rServer {} online_secs {} connections {:#?} con_avg {:#?} con_best {:#?}", server.hostname, server.online_since.read().unwrap().elapsed().as_secs(), server.conn_count, con_avg, if con_avg != 0 { server.conn_best.load(Ordering::Relaxed) } else { 0 });
      }
      info_last = Instant::now();
    }
  }
}

fn select_server(pool: &[usize], host: &str) -> Result<usize, &'static str> {
  let mut hasher = DefaultHasher::new();
  host.hash(&mut hasher);
  let hash = hasher.finish() as usize;
  let serverno = pool[hash%pool.len()];
  Ok(serverno)
}

fn cleanup(error: &str) {
  if !error.is_empty() { println!("\r{}", error); }
  let threads = THREAD_COUNT.fetch_sub(1, Ordering::SeqCst);
  print!("\r{:3} connections | ", threads-1);
  stdout().flush().expect("Failed to flush stdout");
}
