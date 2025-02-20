namespace db;

entity Messages {
  key ID: UUID;
  data: String;
  headers: String;
}
