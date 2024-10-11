SELECT
 @idpdam,
 @id := @id + 1 AS idpelangganair,
 pel.nosamb,
 pel.norekening,
 pel.nama,
 pel.alamat,
 pel.rt,
 pel.rw,
 IFNULL(gol.id, -1) AS idgolongan,
 IFNULL(dia.id, -1) AS iddiameter,
 -1 AS idjenispipa,
 IFNULL(mer.id, -1) AS idkwh,
 IFNULL(ray.id, -1) AS idrayon,
 IFNULL(kel.id, -1) AS idkelurahan,
 IFNULL(kol.id, -1) AS idkolektif,
 pel.status AS idstatus,
 pel.flag AS idflag,
 NULL AS latitude,
 NULL AS longitude,
 NULL AS alamatmap,
 NULL AS fotorumah1,
 NULL AS fotorumah2,
 NULL AS fotorumah3,
 NULL AS fotokwh,
 NULL AS fotodenah1,
 NULL AS fotodenah2,
 999 AS akurasi,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 pelanggan pel
 LEFT JOIN golongan gol ON gol.kodegol = pel.kodegol AND gol.aktif = 1
 LEFT JOIN diameter dia ON dia.kodediameter = pel.kodediameter AND dia.aktif = 1
 LEFT JOIN merkmeter mer ON mer.merk = pel.merkmeter
 LEFT JOIN rayon ray ON ray.koderayon = pel.koderayon
 LEFT JOIN kelurahan kel ON kel.kodekelurahan = pel.kodekelurahan
 LEFT JOIN kolektif kol ON kol.kodekolektif = pel.kodekolektif
 ,(SELECT @id := @lastId) AS id;