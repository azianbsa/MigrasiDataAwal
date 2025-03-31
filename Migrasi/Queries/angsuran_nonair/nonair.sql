DROP TEMPORARY TABLE IF EXISTS __tmp_golongan;
CREATE TEMPORARY TABLE __tmp_golongan AS
SELECT
@id:=@id+1 AS id,
kodegol,
aktif
FROM
golongan,
(SELECT @id:=0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_nonair;
CREATE TEMPORARY TABLE __tmp_nonair AS
SELECT 
a.id AS idangsuran,
a.jumlahtermin,
a.noangsuran AS noangsuran1,
b.*
FROM `daftarangsuran` a
JOIN nonair b ON b.`urutan`=a.`urutan_nonair`
WHERE a.`keperluan`<>'JNS-36' 
AND b.jenis<>'JNS-38'
AND (DATE(a.waktuupload)=@cutoff OR a.flagupload=0);

SELECT
@idpdam,
na.id AS idnonair,
jns.idjenisnonair AS idjenisnonair,
pel.id AS idpelangganair,
NULL AS idpelangganlimbah,
NULL AS idpelangganlltt,
IF(na.periode='',NULL,na.periode) AS kodeperiode,
na.urutan AS nomornonair,
na.keterangan AS keterangan,
na.total AS total,
na.tglmulaitagih AS tanggalmulaitagih,
na.validdate AS tanggalkadaluarsa,
na.nama AS nama,
na.alamat AS alamat,
ryn.id AS idrayon,
NULL AS idkelurahan,
gol.id AS idgolongan,
NULL AS idtariflimbah,
NULL AS idtariflltt,
na.flagangsur AS flagangsur,
na.`idangsuran` AS idangsuran,
na.`jumlahtermin` AS termin,
na.kwitansimanual AS flagmanual,
NULL AS idpermohonansambunganbaru,
na.flaghapus AS flaghapus,
NULL AS `iduser`,
COALESCE(na.waktuupdate,NOW()) AS waktuupdate,
COALESCE(na.waktuinput,na.waktuupdate) AS `created_at`
FROM `__tmp_nonair` na
LEFT JOIN pelanggan pel ON pel.nosamb=na.dibebankankepada
LEFT JOIN __tmp_jenisnonair jns ON jns.kodejenisnonair=na.jenis
LEFT JOIN __tmp_golongan gol ON gol.kodegol=na.kodegol AND gol.aktif=1
LEFT JOIN [bsbs].rayon ryn ON ryn.koderayon=na.koderayon