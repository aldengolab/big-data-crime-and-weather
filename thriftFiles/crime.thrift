namespace java agolab.CrimeAndWeatherApp.crimeEvent

struct CrimeEvent {
  1: required i64 ID;
  2: required i64 year;
  3: required i64 month;
  4: required i64 day;
  5: required i64 hour;
  6: required string primaryType;
  7: required string description;
  8: required string city;
  9: required string state;
  10: optional i64 latitude;
  11: optional i64 longitude;
}