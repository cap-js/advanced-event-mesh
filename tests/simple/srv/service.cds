using { db } from '../db/schema';
service Foo {
  entity Messages as projection on db.Messages;
}
