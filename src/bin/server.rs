use diy_redis::db::ShardedDb;
use mini_redis::{Command, Connection, Frame};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let db: ShardedDb = ShardedDb::new();

    loop {
        let (socket, _) = listener.accept().await.unwrap();

        let db = db.clone();

        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

async fn process(socket: TcpStream, mut db: ShardedDb) {
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        println!("GOT: {:?}", frame);
        let response = match Command::from_frame(frame).unwrap() {
            Command::Get(cmd) => match db.get(cmd.key()) {
                Some(val) => Frame::Bulk(val.clone()),
                None => Frame::Null,
            },
            Command::Set(cmd) => {
                db.insert(cmd.key(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            _ => todo!(),
        };

        connection.write_frame(&response).await.unwrap();
    }
}
