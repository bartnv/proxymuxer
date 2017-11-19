extern crate yaml_rust;

use std::fs::File;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, Ipv4Addr, Ipv6Addr};
use std::time::{Duration, Instant};
use std::thread;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, ATOMIC_USIZE_INIT};
use std::sync::atomic::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use yaml_rust::YamlLoader;

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

static THREAD_COUNT: AtomicUsize = ATOMIC_USIZE_INIT;

fn main() {
  let mut app = App { listenport: 8080, startport: 61234, sshargs: String::from("-N") };
  let mut file = File::open("config.yml").unwrap();
  let mut config_str = String::new();
  file.read_to_string(&mut config_str).unwrap();
  let docs = YamlLoader::load_from_str(&config_str).unwrap();
  let config = &docs[0];
  let io_timeout = Duration::new(300, 0); // 5 minutes

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

  let server = TcpListener::bind(("0.0.0.0", app.listenport as u16)).unwrap();
  for client in server.incoming() {
    let mut stream = client.unwrap();
    stream.set_read_timeout(Some(io_timeout)).expect("Failed to set read timeout on TcpStream");
    stream.set_write_timeout(Some(io_timeout)).expect("Failed to set write timeout on TcpStream");
    let addr = stream.peer_addr().unwrap();
    let servers = servers.clone();
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
            let idx;
            if req[3] == b'\x01' { // IPv4
              let address = Ipv4Addr::new(req[4], req[5], req[6], req[7]);
              host = format!("{}", address);
              port = req[8] as u16;
              port += req[9] as u16 >> 8;
              match select_server(&threads, &servers, &host, port) {
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
              match select_server(&threads, &servers, &host, port) {
                Ok(i) => idx = i,
                Err(msg) => {
                  println!("{}", msg);
                  return;
                }
              }
            }
            else if req[3] == b'\x04' { // IPv6
              let mut address: [u8; 16] = Default::default();
              address.copy_from_slice(&req[4..20]);
              host = format!("{}", Ipv6Addr::from(address));
              port = req[20] as u16;
              port += req[21] as u16 >> 8;
              match select_server(&threads, &servers, &host, port) {
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
            let server = servers.get(idx).expect("selectServer() returned invalid index");
            let mut tunnel = TcpStream::connect(("127.0.0.1", server.portno as u16)).expect("Failed to connect to tunnel port");
            tunnel.set_read_timeout(Some(io_timeout)).expect("Failed to set read timeout on TcpStream");
            tunnel.set_write_timeout(Some(io_timeout)).expect("Failed to set write timeout on TcpStream");
            let _ = tunnel.write(b"\x05\x01\x00").unwrap();
            let mut buf: [u8; 1500] = [0; 1500];
            let _ = tunnel.read(&mut buf).unwrap(); // TODO: check the SOCKS5 response here
            let _ = tunnel.write(&req[0..c]).unwrap();
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
              Ok(c) => println!("Host {} port {} finished with {}b headers {}b data {}s duration", host, port, count, c, start.elapsed().as_secs()),
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

fn select_server(theads: &usize, servers: &Vec<Server>, host: &str, port: u16) -> Result<usize, &'static str> {
  let mut hasher = DefaultHasher::new();
  host.hash(&mut hasher);
  let hash = hasher.finish() as usize;
  let serverno = hash%servers.len();
  let server = servers.get(serverno).unwrap();
  println!("{} Host {} port {} selected server {} ({:?}) of {}", theads, host, port, serverno+1, server, servers.len());
  if server.online.load(Ordering::Relaxed) != true {
    return Err("Selected server is offline");
  }
  Ok(serverno)
}
