DELETE FROM `testdb`.`student` WHERE `id`=2;
DELETE FROM `testdb`.`student` WHERE `id`=6;
DELETE FROM `testdb`.`student` WHERE `id`=7;
DELETE FROM `testdb`.`student` WHERE `id`=8;
DELETE FROM `testdb`.`student` WHERE `id`=1228;
INSERT INTO `testdb`.`student` (`id`,`number`,`name`,`add_time`,`content`) VALUES (1229,20,'chenxi','2020-07-11 16:20:50',null);
INSERT INTO `testdb`.`student` (`id`,`number`,`name`,`add_time`,`content`) VALUES (1231,21,'chenxi','2020-07-12 10:12:45',null);
UPDATE `testdb`.`student` SET `number`=22, `content`=null WHERE `id`=9;
DELETE FROM `testdb`.`student` WHERE `id`=10;
