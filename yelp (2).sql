-- phpMyAdmin SQL Dump
-- version 4.5.5.1
-- http://www.phpmyadmin.net
--
-- Host: localhost
-- Generation Time: Mar 31, 2016 at 06:26 PM
-- Server version: 5.7.11
-- PHP Version: 5.6.19

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `yelp`
--

-- --------------------------------------------------------

--
-- Table structure for table `attributes`
--

CREATE TABLE `attributes` (
  `id` int(11) NOT NULL,
  `attribute` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `attribute_business_link`
--

CREATE TABLE `attribute_business_link` (
  `id` int(11) NOT NULL,
  `business_id` varchar(22) NOT NULL,
  `attr_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `business`
--

CREATE TABLE `business` (
  `business_id` varchar(22) NOT NULL,
  `full_address` text NOT NULL,
  `open` tinyint(1) NOT NULL,
  `city` varchar(255) NOT NULL,
  `state` varchar(255) NOT NULL,
  `latitude` double NOT NULL,
  `longitude` double NOT NULL,
  `review_count` int(11) NOT NULL,
  `name` text NOT NULL,
  `neighborhoods` text NOT NULL,
  `stars` float NOT NULL,
  `type` varchar(255) NOT NULL DEFAULT 'business'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `categories`
--

CREATE TABLE `categories` (
  `cat_id` int(11) NOT NULL,
  `cat_name` varchar(255) NOT NULL,
  `parent_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `category_business_link`
--

CREATE TABLE `category_business_link` (
  `id` int(11) NOT NULL,
  `business_id` varchar(22) NOT NULL,
  `cat_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `checkins`
--

CREATE TABLE `checkins` (
  `id` int(11) NOT NULL,
  `business_id` varchar(22) NOT NULL,
  `checkin_hour` int(11) NOT NULL,
  `checkin_day_id` int(11) NOT NULL,
  `number` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `days`
--

CREATE TABLE `days` (
  `day_id` int(1) NOT NULL,
  `day` varchar(30) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `hours`
--

CREATE TABLE `hours` (
  `business_id` varchar(22) NOT NULL,
  `id` int(11) NOT NULL,
  `day_id` int(11) NOT NULL,
  `open` time NOT NULL,
  `close` time NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `reviews`
--

CREATE TABLE `reviews` (
  `review_id` varchar(22) NOT NULL,
  `user_id` varchar(22) NOT NULL,
  `business_id` varchar(22) NOT NULL,
  `votes_useful` int(11) NOT NULL,
  `votes_funny` int(11) NOT NULL,
  `votes_cool` int(11) NOT NULL,
  `starts` int(11) NOT NULL,
  `date` date NOT NULL,
  `text` text NOT NULL,
  `type` varchar(20) NOT NULL DEFAULT 'review'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `users`
--

CREATE TABLE `users` (
  `user_id` varchar(22) NOT NULL,
  `votes_funny` int(11) NOT NULL,
  `votes_useful` int(11) NOT NULL,
  `votes_cool` int(11) NOT NULL,
  `review_count` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `fans` int(11) NOT NULL,
  `avg_starts` double NOT NULL,
  `type` varchar(20) NOT NULL DEFAULT 'user'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `attributes`
--
ALTER TABLE `attributes`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `attribute` (`attribute`);

--
-- Indexes for table `attribute_business_link`
--
ALTER TABLE `attribute_business_link`
  ADD PRIMARY KEY (`id`),
  ADD KEY `business_id` (`business_id`),
  ADD KEY `attr_id` (`attr_id`);

--
-- Indexes for table `business`
--
ALTER TABLE `business`
  ADD PRIMARY KEY (`business_id`);

--
-- Indexes for table `categories`
--
ALTER TABLE `categories`
  ADD PRIMARY KEY (`cat_id`),
  ADD KEY `parent_id` (`parent_id`);

--
-- Indexes for table `category_business_link`
--
ALTER TABLE `category_business_link`
  ADD PRIMARY KEY (`id`),
  ADD KEY `business_id` (`business_id`),
  ADD KEY `cat_id` (`cat_id`);

--
-- Indexes for table `checkins`
--
ALTER TABLE `checkins`
  ADD PRIMARY KEY (`id`),
  ADD KEY `business_id` (`business_id`),
  ADD KEY `checkin_day_id` (`checkin_day_id`);

--
-- Indexes for table `days`
--
ALTER TABLE `days`
  ADD PRIMARY KEY (`day_id`);

--
-- Indexes for table `hours`
--
ALTER TABLE `hours`
  ADD PRIMARY KEY (`id`),
  ADD KEY `business_id_hours` (`business_id`),
  ADD KEY `day_id` (`day_id`);

--
-- Indexes for table `reviews`
--
ALTER TABLE `reviews`
  ADD PRIMARY KEY (`review_id`),
  ADD KEY `user_id` (`user_id`),
  ADD KEY `business_id` (`business_id`);

--
-- Indexes for table `users`
--
ALTER TABLE `users`
  ADD PRIMARY KEY (`user_id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `attributes`
--
ALTER TABLE `attributes`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=462793;
--
-- AUTO_INCREMENT for table `attribute_business_link`
--
ALTER TABLE `attribute_business_link`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=190021;
--
-- AUTO_INCREMENT for table `categories`
--
ALTER TABLE `categories`
  MODIFY `cat_id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2279;
--
-- AUTO_INCREMENT for table `category_business_link`
--
ALTER TABLE `category_business_link`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=135028;
--
-- AUTO_INCREMENT for table `checkins`
--
ALTER TABLE `checkins`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `days`
--
ALTER TABLE `days`
  MODIFY `day_id` int(1) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=8;
--
-- AUTO_INCREMENT for table `hours`
--
ALTER TABLE `hours`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=75238;
--
-- Constraints for dumped tables
--

--
-- Constraints for table `attribute_business_link`
--
ALTER TABLE `attribute_business_link`
  ADD CONSTRAINT `attribute_business_link_ibfk_1` FOREIGN KEY (`business_id`) REFERENCES `business` (`business_id`),
  ADD CONSTRAINT `attribute_business_link_ibfk_2` FOREIGN KEY (`attr_id`) REFERENCES `attributes` (`id`);

--
-- Constraints for table `categories`
--
ALTER TABLE `categories`
  ADD CONSTRAINT `parent_cat_const` FOREIGN KEY (`parent_id`) REFERENCES `categories` (`cat_id`);

--
-- Constraints for table `category_business_link`
--
ALTER TABLE `category_business_link`
  ADD CONSTRAINT `category_business_link_ibfk_1` FOREIGN KEY (`business_id`) REFERENCES `business` (`business_id`),
  ADD CONSTRAINT `category_business_link_ibfk_2` FOREIGN KEY (`cat_id`) REFERENCES `categories` (`cat_id`);

--
-- Constraints for table `checkins`
--
ALTER TABLE `checkins`
  ADD CONSTRAINT `checkins_ibfk_1` FOREIGN KEY (`business_id`) REFERENCES `business` (`business_id`),
  ADD CONSTRAINT `checkins_ibfk_2` FOREIGN KEY (`checkin_day_id`) REFERENCES `days` (`day_id`);

--
-- Constraints for table `hours`
--
ALTER TABLE `hours`
  ADD CONSTRAINT `b_id_const` FOREIGN KEY (`business_id`) REFERENCES `business` (`business_id`),
  ADD CONSTRAINT `day_id_const` FOREIGN KEY (`day_id`) REFERENCES `days` (`day_id`);

--
-- Constraints for table `reviews`
--
ALTER TABLE `reviews`
  ADD CONSTRAINT `reviews_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `users` (`user_id`),
  ADD CONSTRAINT `reviews_ibfk_2` FOREIGN KEY (`business_id`) REFERENCES `business` (`business_id`);

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
