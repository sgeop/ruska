extern crate protobuf;
extern crate kafka;
extern crate lmdb_rs as lmdb;

mod kafka_message;
mod storage;

use std::thread;
use std::sync::mpsc;
use std::convert::From;
use kafka::client::KafkaClient;
use kafka_message::KafkaMessage;

fn new_message(topic: &'static str, key: &'static str, value: Vec<u8>) -> KafkaMessage {
    let mut message = KafkaMessage::new();
    message.set_topic(topic.to_owned());
    message.set_key(key.to_owned());
    message.set_value(value);
    message
}


fn main() {
    let broker = "sandbox.hortonworks.com:6667";


    let (tx, rx) = mpsc::channel::<KafkaMessage>();

    for _ in 1..10 {
        let msg = new_message("topic", "key", "blah blah".to_owned().into_bytes());
        tx.send(msg);
    }
    drop(tx);

    let handle = thread::spawn(move || {
        let mut client = KafkaClient::new(vec!(broker.to_owned()));
        if let Err(e) = client.load_metadata_all() {
            println!("failed to load metadata from {}: {}", broker, e);
            return;
        };

        while let Ok(msg) = rx.recv() {
            let topic = msg.get_topic().to_owned();
            let value: Vec<u8> = From::from(msg.get_value());
            if let Err(e) = client.send_message(1, 100, topic, value) {
                panic!(e);
            };
        }
    });

    handle.join().unwrap();
}



    

