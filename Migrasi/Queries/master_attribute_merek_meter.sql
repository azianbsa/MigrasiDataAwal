SELECT
 @idpdam,
 id AS idmerekmeter,
 id AS kodemerekmeter,
 merk AS namamerekmeter,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 merkmeter;