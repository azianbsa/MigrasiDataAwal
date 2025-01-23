DROP TABLE IF EXISTS __tmp_jnsbangunan;

CREATE TABLE __tmp_jnsbangunan (
 idpdam SMALLINT (6) NOT NULL,
 idjenisbangunan INT (11) NOT NULL,
 namajenisbangunan VARCHAR (50),
 flaghapus TINYINT (1) DEFAULT 0,
 waktuupdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY (idpdam, idjenisbangunan)
) ENGINE = INNODB;

INSERT INTO __tmp_jnsbangunan(idpdam,idjenisbangunan,namajenisbangunan) VALUES
(@idpdam,-1,'-'),
(@idpdam,1,'BERTINGKAT'),
(@idpdam,2,'TIDAK BERTINGKAT'),
(@idpdam,3,'RUKO');

SELECT
 idpdam,
 idjenisbangunan,
 namajenisbangunan,
 flaghapus,
 waktuupdate
FROM
 __tmp_jnsbangunan
 WHERE idpdam=@idpdam;
 
DROP TABLE __tmp_jnsbangunan;