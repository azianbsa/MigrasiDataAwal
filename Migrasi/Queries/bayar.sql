DROP TEMPORARY TABLE IF EXISTS temp_dataawal_periode;
CREATE TEMPORARY TABLE temp_dataawal_periode (
    idperiode INT,
    periode VARCHAR(10),
    INDEX idx_temp_dataawal_periode_periode (periode)
);
INSERT INTO temp_dataawal_periode
SELECT
@idperiode := @idperiode+1 AS idperiode,
periode
FROM [bsbs].periode
,(SELECT @idperiode := 0) AS idperiode
ORDER BY periode;

SELECT
 @idpdam,
 @id:=@id+1 AS idrekeningair,
 pel.id AS idpelangganair,
 per.idperiode AS idperiode,
 gol.id AS idgolongan,
 dia.id AS iddiameter,
 1 AS idjenispipa,
 1 AS idkwh,
 ray.id AS idrayon,
 kel.id AS idkelurahan,
 kol.id AS idkolektif,
 adm.id AS idadministrasilain,
 pem.id AS idpemeliharaanlain,
 ret.id AS idretribusilain,
 1 AS idstatus,
 1 AS idflag,
 rek.stanlalu AS stanlalu,
 IFNULL(rek.stanskrg, 0) AS stanskrg,
 IFNULL(rek.stanangkat, 0) AS stanangkat,
 IFNULL(rek.pakai, 0) AS pakai,
 0 AS pakaikalkulasi,
 IFNULL(rek.biayapemakaian, 0) AS biayapemakaian,
 IFNULL(rek.administrasi, 0) AS administrasi,
 IFNULL(rek.pemeliharaan, 0) AS pemeliharaan,
 IFNULL(rek.retribusi, 0) AS retribusi,
 IFNULL(rek.pelayanan, 0) AS pelayanan,
 IFNULL(rek.airlimbah, 0) AS airlimbah,
 IFNULL(rek.dendapakai0, 0) AS dendapakai0,
 IFNULL(rek.administrasilain, 0) AS administrasilain,
 IFNULL(rek.pemeliharaanlain, 0) AS pemeliharaanlain,
 IFNULL(rek.retribusilain, 0) AS retribusilain,
 IFNULL(rek.ppn, 0) AS ppn,
 IFNULL(rek.meterai, 0) AS meterai,
 IFNULL(rek.rekair, 0) AS rekair,
 IFNULL(rek.dendatunggakan, 0) AS denda,
 0 AS diskon,
 0 AS deposit,
 IFNULL(rek.total, 0) AS total,
 0 AS hapussecaraakuntansi,
 NULL AS waktuhapussecaraakuntansi,
 NULL AS iddetailcyclepembacaan,
 NULL AS tglpenentuanbaca,
 1 AS flagbaca,
 0 AS metodebaca,
 DATE(NOW()) AS waktubaca,
 DATE_FORMAT(NOW(), '%H:%i:%s') AS jambaca,
 pbc.kodepetugas AS petugasbaca,
 kln.idkelainan AS kelainan,
 0 AS stanbaca,
 DATE(NOW()) AS waktukirimhasilbaca,
 DATE_FORMAT(NOW(), '%H:%i:%s') AS jamkirimhasilbaca,
 NULL AS memolapangan,
 NULL AS lampiran,
 0 AS taksasi,
 0 AS taksir,
 0 AS flagrequestbacaulang,
 NULL AS waktuupdaterequestbacaulang,
 1 AS flagkoreksi,
 DATE(NOW()) AS waktukoreksi,
 DATE_FORMAT(NOW(), '%H:%i:%s') AS jamkoreksi,
 1 AS flagverifikasi,
 DATE(NOW()) AS waktuverifikasi,
 DATE_FORMAT(NOW(), '%H:%i:%s') AS jamverifikasi,
 1 AS flagpublish,
 NOW() AS waktupublish,
 DATE_FORMAT(NOW(), '%H:%i:%s') AS jampublish,
 NULL AS latitude,
 NULL AS longitude,
 NULL AS latitudebulanlalu,
 NULL AS longitudebulanlalu,
 1 AS adafotometer,
 0 AS adafotorumah,
 0 AS adavideo,
 0 AS flagminimumpakai,
 0 AS pakaibulanlalu,
 0 AS pakai2bulanlalu,
 0 AS pakai3bulanlalu,
 0 AS pakai4bulanlalu,
 0 AS persentasebulanlalu,
 0 AS persentase2bulanlalu,
 0 AS persentase3bulanlalu,
 NULL AS kelainanbulanlalu,
 NULL AS kelainan2bulanlalu,
 rek.flagangsur AS flagangsur,
 NULL AS idangsuran,
 NULL AS idmodule,
 0 AS flagkoreksibilling,
 rek.tglmulaidenda AS tglmulaidenda1,
 rek.tglmulaidenda2 AS tglmulaidenda2,
 rek.tglmulaidenda3 AS tglmulaidenda3,
 rek.tglmulaidenda4 AS tglmulaidenda4,
 rek.tglmulaidendaperbulan AS tglmulaidendaperbulan,
 NULL AS tglmulaidendaperhari,
 0 AS flaghasbeenpublish,
 0 AS flagdrdsusulan,
 NULL AS waktudrdsusulan,
 NOW() AS waktuupdate,
 0 AS flaghapus
FROM
 [table] rek
 JOIN [bsbs].pelanggan pel ON pel.nosamb = rek.nosamb
 JOIN temp_dataawal_periode per ON per.periode = rek.periode
 LEFT JOIN [bsbs].golongan gol ON gol.kodegol = rek.kodegol AND gol.aktif = 1
 LEFT JOIN [bsbs].diameter dia ON dia.kodediameter = rek.kodediameter AND dia.aktif = 1
 LEFT JOIN [bsbs].rayon ray ON ray.koderayon = rek.koderayon
 LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan = pel.kodekelurahan
 LEFT JOIN [bsbs].kolektif kol ON kol.kodekolektif = rek.kodekolektif
 LEFT JOIN [bsbs].byadministrasi_lain adm ON adm.kode = rek.kodeadministrasilain
 LEFT JOIN [bsbs].bypemeliharaan_lain pem ON pem.kode = rek.kodepemeliharaanlain
 LEFT JOIN [bsbs].byretribusi_lain ret ON ret.kode = rek.koderetribusilain
 LEFT JOIN [bsbs].pembacameter pbc ON pbc.nama = TRIM(SUBSTRING_INDEX(rek.pembacameter, '(', 1))
 LEFT JOIN [bsbs].kelainan kln ON kln.kelainan = rek.kelainan
 ,(SELECT @id := @lastid) AS id
 WHERE rek.periode = @periode
 AND rek.kode = CONCAT(rek.periode, '.', rek.nosamb)
 AND rek.flaglunas = 1
 AND rek.flagbatal = 0
 AND rek.flagangsur = 0