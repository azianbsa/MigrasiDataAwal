REPLACE INTO cabang (kodecabang, cabang)
VALUES ('-', '-');

REPLACE INTO kecamatan (
 kodekecamatan,
 kecamatan,
 kodecabang
)
VALUES
 (
  '-',
  '-',
  '-'
 );

UPDATE [table] SET kodekelurahan='-' WHERE kodekelurahan='';

REPLACE INTO kelurahan (
 kodekelurahan
)
VALUES
 (
  '-'
 );