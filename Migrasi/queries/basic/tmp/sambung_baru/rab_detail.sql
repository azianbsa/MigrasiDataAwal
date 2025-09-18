SET @tgl_reg_awal='2014-11-21';
SET @tgl_reg_akhir='2025-06-20';
SET @lastid=2355;

SELECT
@id:=@id+1 AS id,
@idpdam AS idpdam,
b.`idpermohonan` AS idpermohonan,
c.tipe AS tipe,
c.kode AS kode,
c.uraian AS uraian,
c.hargasatuan AS hargasatuan,
c.satuan AS satuan,
c.qty AS qty,
c.jumlah jumlah,
0 AS ppn,
0 AS keuntungan,
0 AS jasadaribahan,
c.jumlah AS total,
c.kategori AS kategori,
c.kelompok AS kelompok,
c.postbiaya AS postbiaya,
c.qty AS qtyrkp,
0 AS flagbiayadibebankankepdam,
0 AS flagdialihkankevendor,
1 AS flagpaket,
0 AS flagdistribusi,
0 AS untuksppbdarispk,
a.`tgl_rab` AS waktuupdate
FROM `t_pelanggan_reg` a
JOIN `sambunganbaru` b ON b.`no_reg`=a.`no_reg`
JOIN (
SELECT * FROM `maros_awal`.`rabmaros` WHERE namapaket='PAKET RAB SAM BARU 1/2'
) c ON 1=1
,(SELECT @id:=@lastid) AS id
WHERE a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
AND a.`no_rab`<>'-'
AND a.no_reg IN (
'0005/PMP/HL/VI/2025'
)