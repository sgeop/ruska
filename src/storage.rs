use std::sync::Arc;
use std::path::Path;
use lmdb;

pub struct Client {
    context: Arc<Context>
}

struct Context {
    pub env: lmdb::Environment,
    pub db_handle: lmdb::DbHandle
}

impl Client {
    pub fn new(path: &str, db_flags: lmdb::DbFlags) -> Client {
        let path = Path::new(path);

        let env = lmdb::EnvBuilder::new().open(path, 0o777).unwrap();
        let db_handle =  env.get_default_db(db_flags).unwrap();

        let context = Arc::new(Context {
            env: env,
            db_handle: db_handle,
        });

        Client { context: context }
    }

    pub fn get_context(&self) -> Arc<Context> {
        self.context.clone()
    }
}

impl Context {
    pub fn with_read_write(&self, f: &Fn(lmdb::Database) -> ()) -> Result<(), lmdb::MdbError> {
        let txn = self.env.new_transaction().unwrap();
        {
            let db = txn.bind(&self.db_handle);
            f(db)
        }
        txn.commit()
    }

    pub fn with_readonly(&self, f: &Fn(lmdb::Database) -> ()) {
        let reader = self.env.get_reader().unwrap();
        let db = reader.bind(&self.db_handle);
        f(db)
    }
}

#[test]
fn stroage() {
    let client = Client::new(&"test-lmdb", lmdb::DbFlags::empty());
    let context = client.get_context();

    let res = context.with_read_write(&|db| {
        let pairs = vec![("Albert", "Einstein",),
                         ("Adam", "Smith",),
                         ("Jack", "Daniels")];

        for &(name, surname) in pairs.iter() {
            db.set(&surname, &name).unwrap();
        }
    });
    match res {
        Err(_) => panic!("commit failed!"),
        Ok(_) => ()
    }

    // Note: `commit` is choosen to be explicit as
    // in case of failure it is responsibility of
    // the client to handle the error

    context.with_readonly(&|db| {
        let name: &str = db.get::<&str>(&"Smith").unwrap();
        assert_eq!(name, "Smith");
    });
}
