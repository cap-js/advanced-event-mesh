service ExtSrv {
  @topic: 'cap.external.object.changed.v1'
  event changed {
    key ID1    : Integer;
        value1 : Integer;
  }
}
