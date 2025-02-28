DROP TEMPORARY TABLE IF EXISTS __tmp_golongan;
CREATE TEMPORARY TABLE __tmp_golongan AS
SELECT
@id:=@id+1 AS id,
kodegol,
aktif
FROM
golongan,
(SELECT @id:=0) AS id;

SELECT
@idpdam,
na.id AS idnonair,
jns.idjenisnonair AS idjenisnonair,
pel.id AS idpelangganair,
NULL AS idpelangganlimbah,
NULL AS idpelangganlltt,
IF(na.periode='',NULL,na.periode) AS kodeperiode,
na.nomor AS nomornonair,
na.keterangan AS keterangan,
na.total AS total,
na.tglmulaitagih AS tanggalmulaitagih,
NULL AS tanggalkadaluarsa,
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
0 AS flagmanual,
NULL AS idpermohonansambunganbaru,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
[table] na
LEFT JOIN pelanggan pel ON pel.nosamb = na.dibebankankepada
LEFT JOIN __tmp_jenisnonair jns ON jns.kodejenisnonair = na.jenis
LEFT JOIN [bsbs].rayon ryn ON ryn.koderayon = na.koderayon
LEFT JOIN __tmp_golongan gol ON gol.kodegol = na.kodegol AND gol.aktif = 1
WHERE jenis<>'JNS-16' AND na.periode = @periode OR na.periode IS NULL OR na.periode = ''