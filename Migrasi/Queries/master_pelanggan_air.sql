/* column mappings
 * new(0, "idpdam")
 * new(1, "idpelangganair")
 * new(2, "nosamb")
 * new(3, "norekening")
 * new(4, "nama")
 * new(5, "alamat")
 * new(6, "rt")
 * new(7, "rw")
 * new(8, "idgolongan")
 * new(9, "iddiameter")
 * new(10, "idjenispipa")
 * new(11, "idkwh")
 * new(12, "idrayon")
 * new(13, "idkelurahan")
 * new(14, "idkolektif")
 * new(15, "idstatus")
 * new(16, "idflag")
 * new(17, "latitude")
 * new(18, "longitude")
 * new(19, "akurasi")
 * new(20, "nosamblama")
 * new(21, "flaghapus")
 * new(22, "waktuupdate")
 */

SELECT
@idpdam,
p.id AS idpelangganair,
TRIM(p.nosamb) AS nosamb,
TRIM(p.norekening) AS norekening,
TRIM(p.nama) AS nama,
TRIM(p.alamat) AS alamat,
p.rt AS rt,
p.rw AS rw,
g.`idgolongan` AS idgolongan,
d.`iddiameter` AS iddiameter,
-1 AS idjenispipa,
-1 AS idkwh,
r.`idrayon` AS idrayon,
COALESCE(k.`idkelurahan`,-1) AS idkelurahan,
COALESCE(kl.`idkolektif`,-1) AS idkolektif,
p.status AS idstatus,
p.flag AS idflag,
NULL AS latitude,
NULL AS longitude,
999 AS akurasi,
pp.nosamb AS nosamblama,
p.flaghapus AS flaghapus,
p.waktuupdate AS waktuupdate
FROM pelanggan p
LEFT JOIN pelanggan pp ON pp.nosamb_baru=p.nosamb
LEFT JOIN [dataawal].`master_attribute_rayon` r ON r.koderayon=p.koderayon AND r.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_attribute_kelurahan` k ON k.kodekelurahan=p.kodekelurahan AND k.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_tarif_golongan` g ON g.`kodegolongan`=p.kodegol AND g.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_tarif_diameter` d ON d.kodediameter=p.kodediameter AND d.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_attribute_kolektif` kl ON kl.kodekolektif=p.kodekolektif AND kl.`idpdam`=@idpdam