SET @maxId = (SELECT IFNULL(MAX(idroleaccess),0) FROM master_user_role_access);

SELECT
@idpdam,
@id := @id + 1 AS idroleaccess,
c.idrole,
a.idmoduleaccess,
a.value,
0 AS flaghapus,
NOW() AS waktuupdate
FROM (
SELECT idrole,idmoduleaccess,`value`
FROM master_user_role_access 
WHERE idpdam = @idpdamcopy AND flaghapus = 0
AND idrole IN (SELECT idrole FROM master_user_role WHERE idpdam = @idpdamcopy AND flaghapus = 0)
) a
JOIN 
(SELECT idrole,namarole FROM master_user_role WHERE idpdam = @idpdamcopy AND flaghapus = 0) b
ON b.idrole = a.idrole
JOIN
(SELECT idrole,namarole FROM master_user_role WHERE idpdam = @idpdam AND flaghapus = 0) c
ON c.namarole = b.namarole
,(SELECT @id := @maxId) AS id;