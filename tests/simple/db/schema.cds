namespace db;

entity Messages {
  key ID      : UUID;
      event   : String;
      data    : String;
      headers : String;
}
