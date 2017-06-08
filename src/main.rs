extern crate yaml_rust;
extern crate mio;

use std::fs::File;
use std::io::{self, Read};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use std::thread;
use std::process::Command;
use std::collections::HashMap;
use yaml_rust::YamlLoader;
use mio::{Events, Ready, Poll, PollOpt, Token};
use mio::tcp::TcpListener;

const LISTENER: Token = Token(0);

#[derive(Clone)]
struct App {
  listenport: i64,
  startport: i64,
  sshargs: String
}

struct Conn {
  token: Token(),

}

fn main() {
  let mut app = App { listenport: 8080, startport: 61234, sshargs: String::from("-N") };
  let mut file = File::open("config.yml").unwrap();
  let mut config_str = String::new();
  file.read_to_string(&mut config_str).unwrap();
  let docs = YamlLoader::load_from_str(&config_str).unwrap();
  let config = &docs[0];

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

  for server in config["servers"].as_vec().expect("Invalid 'servers' setting in config.yml") {
    let hostname = server.as_str().expect("Invalid entry in 'servers' setting in config.yml").to_owned();
    let portno = app.startport;
    let argstring = app.sshargs.clone();
    app.startport += 1;
    println!("Found server {}", hostname);
    thread::spawn(move || {
      let recon_delay = Duration::new(60, 0);
      loop {
        println!("Connecting to {} with listen port {}", hostname, portno);
        let argvec: Vec<&str> = argstring.split_whitespace().collect();
        let mut child = Command::new("ssh")
                                .arg("-D")
                                .arg(format!("localhost:{}", portno))
                                .args(argvec)
                                .arg(hostname.clone())
                                .spawn().expect(&format!("Failed to launch ssh session to {}", hostname));
        let ecode = child.wait().expect("Failed to wait on child");
        if !ecode.success() {
            match ecode.code() {
                Some(code) => println!("Ssh session for {} failed with exit code {}", hostname, code),
                None => println!("Ssh session for {} was killed by a signal", hostname)
            }
        }
        println!("Waiting {} seconds before reconnecting...", recon_delay.as_secs());
        thread::sleep(recon_delay);
      }
    });
  }

  let poll = Poll::new().unwrap();
  let listener = TcpListener::bind(&SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), app.listenport as u16)).unwrap();
  poll.register(&listener, LISTENER, Ready::readable(), PollOpt::edge()).unwrap();
  let mut events = Events::with_capacity(1024);
  let mut sockets = HashMap::new();
  let mut next_socket_index = 0;

  loop {
    poll.poll(&mut events, None).unwrap();
    for event in &events {
      match event.token() {
        LISTENER => {
          loop {
            match listener.accept() {
              Ok((socket, _)) => {
                let token = Token(next_socket_index);
                next_socket_index += 1;

                poll.register(&socket, token, Ready::readable(), PollOpt::edge()).unwrap();
                sockets.insert(token, socket);
              }
              Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => { break; }
              e => panic!("err={:?}", e)
            }
          }
        }
        token => {
          loop {
            // match sockets.get_mut(&token).unwrap().read(&mut buf) {
            //   Ok(0) => {  // Socket is closed, remove it from the map
            //     sockets.remove(&token);
            //     break;
            //   }
            //   Ok(_) => unreachable!(),
            //   Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => { continue; }
            //   e => panic!("err={:?}", e)
            // }
          }
        }
      }
    }
  }
}
