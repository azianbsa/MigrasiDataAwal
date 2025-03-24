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
@id:=@id+1 AS id,
urutan
FROM nonair
,(SELECT @id:=@lastid) AS id
WHERE flaghapus=0 AND kwitansimanual=1 AND jenis IN (
'JNS-9',
'JNS-23',
'JNS-113',
'JNS-101',
'JNS-43',
'JNS-140',
'JNS-141',
'JNS-143',
'JNS-144',
'JNS-16',
'JNS-145',
'JNS-146',
'JNS-151'
);

SELECT
@idpdam,
n.id AS idnonair,
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
NULL AS idangsuran,
na.termin AS termin,
na.kwitansimanual AS flagmanual,
NULL AS idpermohonansambunganbaru,
0 AS flaghapus,
na.waktuupdate AS waktuupdate
FROM
__tmp_nonair n
JOIN nonair na ON na.urutan=n.urutan
LEFT JOIN pelanggan pel ON pel.nosamb=na.dibebankankepada
LEFT JOIN __tmp_jenisnonair jns ON jns.kodejenisnonair=na.jenis
LEFT JOIN [bsbs].rayon ryn ON ryn.koderayon=na.koderayon
LEFT JOIN __tmp_golongan gol ON gol.kodegol=na.kodegol AND gol.aktif=1