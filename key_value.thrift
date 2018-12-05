exception SystemException {
  1: optional string message
}

struct KeyValue {
  1: i32 key;
  2: string value;
}

struct ValueTime{
  1: string value
  2: double time
  3: string servername
}
service Store {
  string get(1: i32 id, 2: i32 consistency)
    throws (1: SystemException systemException),

  ValueTime getIN(1: i32 id)
    throws (1: SystemException systemException),

  string getHandler(1: i32 index)
    throws (1: SystemException systemException),
  
  bool put(1: KeyValue keyvalue, 2: i32 consistency)
    throws (1: SystemException systemException),

  bool putIN(1: KeyValue keyvalue, 2: double timestamp, 3: string servername)
    throws (1: SystemException systemException),

  bool putHandler(1: i32 index, 2: KeyValue keyvalue, 3: double timestamp)
    throws (1: SystemException systemException),
  
}
