use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::PathBuf;

use chrono::Local;

use crate::crafter::Graph;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum BackendTypes {
    FileStore,
    DataBase,
}

#[derive(Debug, Serialize, Deserialize)]
struct BackendDetails {
    store_dir: PathBuf,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum ExistPolicy {
    Ignore,
    Overwrite,
    Raise,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Flatten {
    store_type: BackendTypes,
    details: BackendDetails,
}

pub trait FlattenStore {
    fn read(self, name: &str) -> Graph;
    fn write(self, data: &Graph) -> bool;

    fn save_as(self, name: &str) -> Self;

    fn on_duplicate(self, policy: ExistPolicy) -> Self;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileStore {
    data_dir: PathBuf,
    if_exists: ExistPolicy,
    name: String,
}

impl Default for FileStore {
    fn default() -> Self {
        Self {
            data_dir: "/tmp/mars_store".into(),
            if_exists: ExistPolicy::Raise,
            name: String::default(),
        }
    }
}

impl FileStore {
    pub fn new() -> Self {
        let instance = Self::default();
        if !instance.data_dir.is_dir() {
            match fs::create_dir_all(&instance.data_dir) {
                Ok(_) => println!("FileStore dir created!"),
                Err(msg) => {
                    eprintln!("Failed to create a filestore dir {:?}", msg);
                    panic!("failed to create!");
                }
            }
        }
        instance
    }

    pub fn dir(mut self, path: &str) -> Self {
        self.data_dir = path.into();
        self
    }

    fn get_abs_filepath(&self) -> PathBuf {
        self.data_dir.join(&self.name)
    }

    fn exists(&self, path: &PathBuf) -> bool {
        path.is_file()
    }
}

impl FlattenStore for FileStore {
    fn read(self, name: &str) -> Graph {
        let src_file = self.data_dir.join(&name);

        if !src_file.is_file() {
            panic!("{:?} file does not exist!", src_file);
        }

        let json_data = fs::read_to_string(src_file).expect("failed to read file!");
        let final_node: Graph =
            serde_json::from_str(json_data.as_str()).expect("failed to deserialize!");

        final_node
    }

    fn write(self, data: &Graph) -> bool {
        let dest_file = self.get_abs_filepath();

        if dest_file.is_file() {
            match self.if_exists {
                ExistPolicy::Raise => {
                    panic!("File {:?} already exists!", dest_file.as_path()); // TODO: Why Panic?
                }
                ExistPolicy::Ignore => {
                    println!(
                        "File {:?} already exist and duplicate policy is set to ignore",
                        dest_file.as_path()
                    );
                    return true;
                }
                ExistPolicy::Overwrite => {
                    println!(
                        "File {:?} already exist and on_duplicate is overwrite",
                        dest_file.as_path()
                    );

                    let curr_ts = Local::now().format("%Y%m%d_%H%M%S").to_string();
                    let new_name = format!("{:?}_{}", dest_file.file_name().unwrap(), curr_ts);

                    let backup_as = dest_file.with_file_name(&new_name);

                    match fs::rename(&dest_file, &new_name) {
                        Ok(_) => {
                            println!("existing file backed up: {:?}", &backup_as);
                        }
                        _ => panic!(
                            "failed to rename existing file {:?} -> {:?}",
                            &dest_file, &new_name
                        ),
                    }
                }
            }
        }

        if dest_file.is_file() {
            match self.if_exists {
                ExistPolicy::Raise => panic!("Duplicate!"),
                ExistPolicy::Ignore => eprintln!("Duplicate, not overwriting..."),
                ExistPolicy::Overwrite => {
                    fs::remove_file(&dest_file).expect("failed to delete existing file...")
                }
            }
        }
        let mut out = fs::File::create_new(&dest_file)
            .expect(format!("Failed to open a file {:?}", &dest_file.as_path()).as_str());

        let json = serde_json::to_string_pretty(&data).expect("Failed to serialize!");

        out.write_all(json.as_bytes())
            .expect("Failed to save serialized model to a file");
        true
    }

    fn save_as(mut self, name: &str) -> Self {
        self.name = name.into();
        self
    }
    fn on_duplicate(mut self, policy: ExistPolicy) -> Self {
        self.if_exists = policy;
        self
    }
}

// impl Flatten {
//     pub fn file_store(policy: ExistPolicy) -> FileStore {
//         FileStore {
//             ..FileStore::default(),
//             if_exist:
//         }
//     }
// }
