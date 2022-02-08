/*
 Navicat Premium Data Transfer

 Source Server         : 0
 Source Server Type    : MySQL
 Source Server Version : 50651
 Source Host           : 192.168.1.0:3306
 Source Schema         : test_db

 Target Server Type    : MySQL
 Target Server Version : 50651
 File Encoding         : 65001

 Date: 08/02/2022 11:38:34
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for test
-- ----------------------------
DROP TABLE IF EXISTS `tests`;
CREATE TABLE `tests`  (
  `guid` int(10) UNSIGNED NOT NULL,
  `value1` bigint(20) UNSIGNED NOT NULL COMMENT '值1',
  `value2` int(10) UNSIGNED NOT NULL COMMENT '值2\r\n值2',
  PRIMARY KEY (`guid`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

SET FOREIGN_KEY_CHECKS = 1;
