/* rekening_air
 * new(0, "idpdam")
 * new(1, "idrekeningair")
 * new(2, "idpelangganair")
 * new(3, "idperiode")
 * new(4, "idgolongan")
 * new(5, "iddiameter")
 * new(6, "idjenispipa")
 * new(7, "idkwh")
 * new(8, "idrayon")
 * new(9, "idkelurahan")
 * new(10, "idkolektif")
 * new(11, "idadministrasilain")
 * new(12, "idpemeliharaanlain")
 * new(13, "idretribusilain")
 * new(14, "idstatus")
 * new(15, "idflag")
 * new(16, "stanlalu")
 * new(17, "stanskrg")
 * new(18, "stanangkat")
 * new(19, "pakai")
 * new(20, "pakaikalkulasi")
 * new(21, "biayapemakaian")
 * new(22, "administrasi")
 * new(23, "pemeliharaan")
 * new(24, "retribusi")
 * new(25, "pelayanan")
 * new(26, "airlimbah")
 * new(27, "dendapakai0")
 * new(28, "administrasilain")
 * new(29, "pemeliharaanlain")
 * new(30, "retribusilain")
 * new(31, "ppn")
 * new(32, "meterai")
 * new(33, "rekair")
 * new(34, "denda")
 * new(35, "diskon")
 * new(36, "deposit")
 * new(37, "total")
 * new(38, "hapussecaraakuntansi")
 * new(39, "waktuhapussecaraakuntansi")
 * new(40, "iddetailcyclepembacaan")
 * new(41, "tglpenentuanbaca")
 * new(42, "flagbaca")
 * new(43, "metodebaca")
 * new(44, "waktubaca")
 * new(45, "jambaca")
 * new(46, "petugasbaca")
 * new(47, "kelainan")
 * new(48, "stanbaca")
 * new(49, "waktukirimhasilbaca")
 * new(50, "jamkirimhasilbaca")
 * new(51, "memolapangan")
 * new(52, "lampiran")
 * new(53, "taksasi")
 * new(54, "taksir")
 * new(55, "flagrequestbacaulang")
 * new(56, "waktuupdaterequestbacaulang")
 * new(57, "flagkoreksi")
 * new(58, "waktukoreksi")
 * new(59, "jamkoreksi")
 * new(60, "flagverifikasi")
 * new(61, "waktuverifikasi")
 * new(62, "jamverifikasi")
 * new(63, "userverifikasi")
 * new(64, "flagpublish")
 * new(65, "waktupublish")
 * new(66, "jampublish")
 * new(67, "userpublish")
 * new(68, "latitude")
 * new(69, "longitude")
 * new(70, "latitudebulanlalu")
 * new(71, "longitudebulanlalu")
 * new(72, "adafotometer")
 * new(73, "adafotorumah")
 * new(74, "adavideo")
 * new(75, "flagminimumpakai")
 * new(76, "pakaibulanlalu")
 * new(77, "pakai2bulanlalu")
 * new(78, "pakai3bulanlalu")
 * new(79, "pakai4bulanlalu")
 * new(80, "persentasebulanlalu")
 * new(81, "persentase2bulanlalu")
 * new(82, "persentase3bulanlalu")
 * new(83, "kelainanbulanlalu")
 * new(84, "kelainan2bulanlalu")
 * new(85, "flagangsur")
 * new(86, "idangsuran")
 * new(87, "idmodule")
 * new(88, "flagkoreksibilling")
 * new(89, "tglmulaidenda1")
 * new(90, "tglmulaidenda2")
 * new(91, "tglmulaidenda3")
 * new(92, "tglmulaidenda4")
 * new(93, "tglmulaidendaperbulan")
 * new(94, "tglmulaidendaperhari")
 * new(95, "flaghasbeenpublish")
 * new(96, "flagdrdsusulan")
 * new(97, "waktudrdsusulan")
 * new(98, "waktuupdate")
 * new(99, "flaghapus")
 */

SELECT
@idpdam,
@id:=@id+1 AS idrekeningair,
pl.id AS idpelangganair,
pr.idperiode AS idperiode,
g.`idgolongan` AS idgolongan,
COALESCE(d.`iddiameter`,-1) AS iddiameter,
-1 AS idjenispipa,
-1 AS idkwh,
r.`idrayon` AS idrayon,
COALESCE(k.`idkelurahan`,-1) AS idkelurahan,
COALESCE(ko.`idkolektif`,-1) AS idkolektif,
-1 AS idadministrasilain,
-1 AS idpemeliharaanlain,
-1 AS idretribusilain,
1 AS idstatus,
p.flag AS idflag,
p.stanlalu AS stanlalu,
IFNULL(p.stanskrg, 0) AS stanskrg,
IFNULL(p.stanangkat, 0) AS stanangkat,
IFNULL(p.pakai, 0) AS pakai,
0 AS pakaikalkulasi,
IFNULL(p.biayapemakaian, 0) AS biayapemakaian,
IFNULL(p.administrasi, 0) AS administrasi,
IFNULL(p.pemeliharaan, 0) AS pemeliharaan,
IFNULL(p.retribusi, 0) AS retribusi,
IFNULL(p.pelayanan, 0) AS pelayanan,
IFNULL(p.airlimbah, 0) AS airlimbah,
IFNULL(p.dendapakai0, 0) AS dendapakai0,
IFNULL(p.administrasilain, 0) AS administrasilain,
IFNULL(p.pemeliharaanlain, 0) AS pemeliharaanlain,
IFNULL(p.retribusilain, 0) AS retribusilain,
IFNULL(p.ppn, 0) AS ppn,
IFNULL(p.meterai, 0) AS meterai,
IFNULL(p.rekair, 0) AS rekair,
IFNULL(p.dendatunggakan, 0) AS denda,
0 AS diskon,
0 AS deposit,
IFNULL(p.total, 0) AS total,
0 AS hapussecaraakuntansi,
NULL AS waktuhapussecaraakuntansi,
NULL AS iddetailcyclepembacaan,
NULL AS tglpenentuanbaca,
1 AS flagbaca,
IF(h.terbaca=1,4,0) AS metodebaca,
DATE(h.waktubaca) AS waktubaca,
DATE_FORMAT(h.waktubaca, '%H:%i:%s') AS jambaca,
h.kodepetugas AS petugasbaca,
kl.idkelainan AS kelainan,
h.datalapangan AS stanbaca,
DATE(h.waktuupload) AS waktukirimhasilbaca,
DATE_FORMAT(h.waktuupload, '%H:%i:%s') AS jamkirimhasilbaca,
NULL AS memolapangan,
NULL AS lampiran,
0 AS taksasi,
0 AS taksir,
0 AS flagrequestbacaulang,
NULL AS waktuupdaterequestbacaulang,
1 AS flagkoreksi,
DATE(h.waktuupload) AS waktukoreksi,
DATE_FORMAT(h.waktuupload, '%H:%i:%s') AS jamkoreksi,
1 AS flagverifikasi,
DATE(h.waktuverifikasi) AS waktuverifikasi,
DATE_FORMAT(h.waktuverifikasi, '%H:%i:%s') AS jamverifikasi,
us.iduser AS userverifikasi,
1 AS flagpublish,
DATE(h.waktuverifikasi) AS waktupublish,
DATE_FORMAT(h.waktuverifikasi, '%H:%i:%s') AS jampublish,
NULL AS userpublish,
h.latitude AS latitude,
h.longitude AS longitude,
NULL AS latitudebulanlalu,
NULL AS longitudebulanlalu,
1 AS adafotometer,
h.adafotorumah AS adafotorumah,
h.adavideo AS adavideo,
0 AS flagminimumpakai,
COALESCE(h.pakai1,0) AS pakaibulanlalu,
COALESCE(h.pakai2,0) AS pakai2bulanlalu,
COALESCE(h.pakai3,0) AS pakai3bulanlalu,
0 AS pakai4bulanlalu,
CAST(REPLACE(IF(h.persen1='-',0,COALESCE(h.persen1,0)),',','.') AS DECIMAL(10,2)) AS persentasebulanlalu,
CAST(REPLACE(IF(h.persen2='-',0,COALESCE(h.persen2,0)),',','.') AS DECIMAL(10,2)) AS persentase2bulanlalu,
CAST(REPLACE(IF(h.persen3='-',0,COALESCE(h.persen3,0)),',','.') AS DECIMAL(10,2)) AS persentase3bulanlalu,
NULL AS kelainanbulanlalu,
NULL AS kelainan2bulanlalu,
p.flagangsur AS flagangsur,
NULL AS idangsuran,
NULL AS idmodule,
0 AS flagkoreksibilling,
p.tglmulaidenda AS tglmulaidenda1,
p.tglmulaidenda2 AS tglmulaidenda2,
p.tglmulaidenda3 AS tglmulaidenda3,
p.tglmulaidenda4 AS tglmulaidenda4,
p.tglmulaidendaperbulan AS tglmulaidendaperbulan,
NULL AS tglmulaidendaperhari,
1 AS flaghasbeenpublish,
0 AS flagdrdsusulan,
NULL AS waktudrdsusulan,
NOW() AS waktuupdate,
0 AS flaghapus
FROM bayar p
JOIN pelanggan pl ON pl.nosamb=p.nosamb
JOIN [dataawal].`master_periode` pr ON pr.`kodeperiode`=p.periode AND pr.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_tarif_golongan` g ON g.`kodegolongan`=p.kodegol AND g.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_tarif_diameter` d ON d.kodediameter=p.kodediameter AND d.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_attribute_rayon` r ON r.koderayon=p.koderayon AND r.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_attribute_kelurahan` k ON k.kodekelurahan=pl.kodekelurahan AND k.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_attribute_kolektif` ko ON ko.kodekolektif=p.kodekolektif AND ko.`idpdam`=@idpdam
LEFT JOIN [bacameter].tampung_hasilbaca h on h.kode=p.kode
LEFT JOIN [dataawal].`master_attribute_kelainan` kl ON kl.`kodekelainan`=h.kodekelainan AND kl.`idpdam`=@idpdam
LEFT JOIN [bacameter].`userakses` u ON u.iduser=h.iduserupdate
LEFT JOIN [dataawal].`master_user` us ON us.nama=u.nama AND us.namauser=u.namauser AND us.idpdam=@idpdam
,(SELECT @id:=@lastid) AS id
WHERE p.periode BETWEEN 202502 AND 202504