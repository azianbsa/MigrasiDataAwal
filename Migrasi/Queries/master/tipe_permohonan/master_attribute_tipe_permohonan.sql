SELECT
 idpdam AS idpdam,
 idtipepermohonan AS idtipepermohonan,
 kodetipepermohonan AS kodetipepermohonan,
 namatipepermohonan AS namatipepermohonan,
 idjenisnonair AS idjenisnonair,
 kategori AS kategori,
 flagpelangganair AS flagpelangganair,
 flagpelangganlimbah AS flagpelangganlimbah,
 flagpelangganlltt AS flagpelangganlltt,
 flagnonpelanggan AS flagnonpelanggan,
 flagpermohonanpelanggannonaktif AS flagpermohonanpelanggannonaktif,
 step_spk AS step_spk,
 step_rab AS step_rab,
 step_spkpasang AS step_spkpasang,
 step_beritaacara AS step_beritaacara,
 step_verifikasi AS step_verifikasi,
 kolektif AS kolektif,
 listselainidstatus AS listselainidstatus,
 idurusan AS idurusan,
 flagaktif AS flagaktif,
 flaghapus AS flaghapus,
 waktuupdate as waktuupdate
FROM
 master_attribute_tipe_permohonan
 WHERE idpdam = @idpdam