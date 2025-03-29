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
FROM [table]
,(SELECT @id:=@lastid) AS id
WHERE flagangsur=0 
AND jenis<>'JNS-38'
AND DATE(COALESCE(waktuinput,waktuupdate))<=@cutoff;

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
na.flaghapus AS flaghapus,
NULL AS `iduser`,
COALESCE(na.waktuupdate,NOW()) AS waktuupdate,
COALESCE(na.waktuinput,na.waktuupdate) AS `created_at`
FROM
__tmp_nonair n
JOIN [table] na ON na.urutan=n.urutan
LEFT JOIN pelanggan pel ON pel.nosamb=na.dibebankankepada
LEFT JOIN __tmp_jenisnonair jns ON jns.kodejenisnonair=na.jenis
LEFT JOIN [bsbs].rayon ryn ON ryn.koderayon=na.koderayon
LEFT JOIN __tmp_golongan gol ON gol.kodegol=na.kodegol AND gol.aktif=1
WHERE na.periode=@periode OR na.periode IS NULL OR na.periode=''