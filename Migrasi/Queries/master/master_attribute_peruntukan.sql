DROP TABLE IF EXISTS __tmp_peruntukan;

CREATE TABLE __tmp_peruntukan (
 idpdam SMALLINT (6) NOT NULL,
 idperuntukan INT (11) NOT NULL,
 namaperuntukan VARCHAR (50),
 flaghapus TINYINT (1) DEFAULT 0,
 waktuupdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY (idpdam, idperuntukan)
) ENGINE = INNODB;

INSERT INTO __tmp_peruntukan(idpdam,idperuntukan,namaperuntukan) VALUES
(@idpdam,-1,'-'),
(@idpdam,0,'Lainnya'),
(@idpdam,1,'RUMAH JASA/KOS/SEWA'),
(@idpdam,2,'RUMAH MEWAH/BERTINGKAT'),
(@idpdam,3,'RUMAH SEDERHANA'),
(@idpdam,4,'RUMAH SANGAT SEDERHANA'),
(@idpdam,5,'FASILITAS UMUM');

SELECT
 idpdam,
 idperuntukan,
 namaperuntukan,
 flaghapus,
 waktuupdate
FROM
 __tmp_peruntukan
 WHERE idpdam=@idpdam;
 
DROP TABLE __tmp_peruntukan;