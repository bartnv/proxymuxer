extern crate yaml_rust;

use std::fs::File;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, Ipv6Addr};
use std::time::Duration;
use std::thread;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
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

  let server = TcpListener::bind(("0.0.0.0", app.listenport as u16)).unwrap();
  for client in server.incoming() {
    let mut stream = client.unwrap();
    stream.set_read_timeout(Some(io_timeout)).expect("Failed to set read timeout on TcpStream");
    stream.set_write_timeout(Some(io_timeout)).expect("Failed to set write timeout on TcpStream");
    let addr = stream.peer_addr().unwrap();
    let servers = servers.clone();
    thread::spawn(move || {
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
          if c == 0 {
            println!("CLOSE from {}", addr);
            return;
          }
          if &req[0..3] == b"\x05\x01\x00" {
            let mut conn = None;
            if req[3] == b'\x01' { // IPv4
              let mut portno = req[8] as u16;
              portno += req[9] as u16 >> 8;
              println!("Requested connection to IPv4 {}.{}.{}.{} port {}", req[4], req[5], req[6], req[7], portno);
            }
            else if req[3] == b'\x03' { // Hostname
              let length = req[4] as usize;
              let hostname = String::from_utf8_lossy(&req[5..(5+length)]);
              let mut portno = (req[5+length] as u16) << 8;
              portno += req[5+length+1] as u16;
              println!("Requested connection to {} port {}", hostname, portno);
              let serverno = 0;
              let server = servers.get(serverno).unwrap();
              println!("Selected server {} ({:?}) of {}", serverno+1, server, servers.len());
              if server.online.load(Ordering::Relaxed) != true {
                println!("Selected server is offline");
                return;
              }
              conn = Some(TcpStream::connect(("127.0.0.1", server.portno as u16)).expect("Failed to connect to tunnel port"));
            }
            else if req[3] == b'\x04' { // IPv6
              let mut address: [u8; 16] = Default::default();
              address.copy_from_slice(&req[4..20]);
              let mut portno = req[20] as u16;
              portno += req[21] as u16 >> 8;
              println!("Requested connection to IPv6 {} port {}", Ipv6Addr::from(address), portno);
            }
            else {
              println!("Invalid connection request from {}", addr);
              return;
            }
            let mut tunnel = conn.unwrap();
            tunnel.set_read_timeout(Some(io_timeout)).expect("Failed to set read timeout on TcpStream");
            tunnel.set_write_timeout(Some(io_timeout)).expect("Failed to set write timeout on TcpStream");
            let _ = tunnel.write(b"\x05\x01\x00").unwrap();
            let mut buf: [u8; 1500] = [0; 1500];
            let _ = tunnel.read(&mut buf).unwrap();
            let _ = tunnel.write(&req[0..c]).unwrap();
            let mut tunnel_read = tunnel.try_clone().expect("Failed to clone tunnel TcpStream");
            let mut stream_write = stream.try_clone().expect("Failed to clone client TcpStream");
            thread::spawn(move || {
              let mut buf: [u8; 1500] = [0; 1500];
              let mut count = 0;
              loop {
                match tunnel_read.read(&mut buf) {
                  Ok(c) => {
                    if c == 0 {
                      println!("Closed after {} bytes inbound", count);
                      return;
                    }
                    count += c;
                    if let Err(_) = stream_write.write_all(&buf[0..c]) {
                      println!("Write error on client");
                      return;
                    }
                  }
                  Err(_) => {
                    println!("Read error on tunnel");
                    return;
                  }
                }
              }
            });
            let mut count = 0;
            loop {
              match stream.read(&mut buf) {
                Ok(c) => {
                  if c == 0 {
                    println!("Closed after {} bytes outbound", count);
                    return;
                  }
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
    });
  }
}
