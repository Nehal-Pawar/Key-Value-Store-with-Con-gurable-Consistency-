exception SystemException {
  1: optional string message
}

struct KeyValue {
  1: i32 key;
  2: string value;
}

service Store {
  string get(1: i32 id)
    throws (1: SystemException systemException),
  
  void put(1: KeyValue keyvalue)
    throws (1: SystemException systemException)
  
}
