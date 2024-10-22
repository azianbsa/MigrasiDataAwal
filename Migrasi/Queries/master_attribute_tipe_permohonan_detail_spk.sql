SELECT
 @idpdam,
 idtipepermohonan,
 parameter,
 tipedata,
 idlistdata,
 isrequired,
 urutan,
 waktuupdate
FROM
 master_attribute_tipe_permohonan_detail_spk
WHERE idpdam=@idpdamcopy