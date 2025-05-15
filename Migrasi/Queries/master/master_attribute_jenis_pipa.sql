DROP TABLE IF EXISTS __tmp_jnspipa;

CREATE TABLE __tmp_jnspipa (
 idpdam SMALLINT (6) NOT NULL,
 idjenispipa INT (11) NOT NULL,
 kodejenispipa VARCHAR (8),
 namajenispipa VARCHAR (50),
 flaghapus TINYINT (1) DEFAULT 0,
 waktuupdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY (idpdam, idjenispipa)
) ENGINE = INNODB;

INSERT INTO __tmp_jnspipa(idpdam,idjenispipa,kodejenispipa,namajenispipa) VALUES
(@idpdam,-1,'-','-'),
(@idpdam,1,'01','Pipa PVC'),
(@idpdam,2,'02','Pipa HDPE'),
(@idpdam,3,'03','Pipa GI'),
(@idpdam,4,'04','Pipa ACP');

SELECT
 idpdam,
 idjenispipa,
 kodejenispipa,
 namajenispipa,
 flaghapus,
 waktuupdate
FROM
 __tmp_jnspipa
 WHERE idpdam=@idpdam;
 
DROP TABLE __tmp_jnspipa;