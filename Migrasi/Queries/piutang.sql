SELECT
 @idpdam,
 @id:=@id+1 AS idrekeningair,
 pel.idpelanggan AS idpelangganair,
 per.idperiode,
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
 rek.flagaktif AS idstatus,
 1 AS idflag,
 rek.stanlalu,
 rek.stanskrg AS stanskrg,
 rek.stanangkat,
 rek.pakai,
 0 AS pakaikalkulasi,
 rek.biayapemakaian,
 rek.administrasi,
 rek.pemeliharaan,
 rek.retribusi,
 rek.pelayanan,
 rek.airlimbah,
 rek.dendapakai0,
 rek.administrasilain,
 rek.pemeliharaanlain,
 rek.retribusilain,
 rek.ppn,
 rek.meterai,
 rek.rekair,
 rek.dendatunggakan AS denda,
 0 AS diskon,
 0 AS deposit,
 rek.total,
 0 AS hapussecaraakuntansi,
 NULL AS waktuhapussecaraakuntansi,
 NULL AS iddetailcyclepembacaan,
 NULL AS tglpenentuanbaca,
 1 AS flagbaca,
 0 AS metodebaca,
 DATE(NOW()) AS waktubaca,
 DATE_FORMAT(NOW(), '%H:%i:%s') AS jambaca,
 pbc.idpetugasbaca AS petugasbaca,
 kln.id AS kelainan,
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
 rek.flagpublish,
 DATE(rek.tglpublish) AS waktupublish,
 DATE_FORMAT(rek.tglpublish, '%H:%i:%s') AS jampublish,
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
 0 AS flagangsur,
 NULL AS idangsuran,
 NULL AS idmodule,
 0 AS flagkoreksibilling,
 rek.tglmulaidenda AS tglmulaidenda1,
 rek.tglmulaidenda2,
 rek.tglmulaidenda3,
 rek.tglmulaidenda4,
 rek.tglmulaidendaperbulan,
 0 AS flaghasbeenpublish,
 0 AS flagdrdsusulan,
 NULL AS waktudrdsusulan,
 NOW() AS waktuupdate,
 0 AS flaghapus
FROM
 piutang rek
 JOIN (
	SELECT
	@idpelanggan:=@idpelanggan+1 AS idpelanggan,
	nosamb
	FROM pelanggan
	,(SELECT @idpelanggan:=0) AS idpelanggan 
	ORDER BY nosamb
 ) pel ON pel.nosamb = rek.nosamb
 JOIN (
	SELECT
	@idperiode:=@idperiode+1 AS idperiode,
	periode
	FROM periode
	,(SELECT @idperiode:=0) AS idperiode
	ORDER BY periode
 ) per ON per.periode = rek.periode
 LEFT JOIN golongan gol ON gol.kodegol = rek.kodegol AND gol.aktif = 1
 LEFT JOIN diameter dia ON dia.kodediameter = rek.kodediameter AND dia.aktif = 1
 LEFT JOIN rayon ray ON ray.koderayon = rek.koderayon
 LEFT JOIN kelurahan kel ON kel.kodekelurahan = rek.kodekelurahan
 LEFT JOIN kolektif kol ON kol.kodekolektif = rek.kodekolektif
 LEFT JOIN byadministrasi_lain adm ON adm.kode = rek.kodeadministrasilain
 LEFT JOIN bypemeliharaan_lain pem ON pem.kode = rek.kodepemeliharaanlain
 LEFT JOIN byretribusi_lain ret ON ret.kode = rek.koderetribusilain
 LEFT JOIN (
	SELECT
	@idpetugasbaca:=@idpetugasbaca+1 AS idpetugasbaca,
	nama
	FROM pembacameter
	,(SELECT @idpetugasbaca:=0) AS idpetugasbaca
 ) pbc ON pbc.nama = TRIM(SUBSTRING_INDEX(rek.pembacameter, '(', 1))
 LEFT JOIN kelainan kln ON kln.kelainan = rek.kelainan
 ,(SELECT @id := 0) AS id;