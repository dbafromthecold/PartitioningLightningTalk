USE [master];
GO

IF EXISTS(SELECT TOP 1 1 FROM sys.databases WHERE name = 'PartitioningDemo')
	DROP DATABASE [PartitioningDemo];
GO


IF NOT EXISTS(SELECT TOP 1 1 FROM sys.databases WHERE name = 'PartitioningDemo')
CREATE DATABASE [PartitioningDemo]
 ON PRIMARY 
(NAME = N'PartitionDemo', FILENAME = N'C:\SQLServer\SQLData\PartitionDemo.mdf', SIZE = 51200KB, MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB), 
 FILEGROUP [DATA] 
(NAME = N'DATA', FILENAME = N'C:\SQLServer\SQLData\DATA.ndf', SIZE = 51200KB, MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB ),
 FILEGROUP [ARCHIVE] 
(NAME = N'ARCHIVE', FILENAME = N'C:\SQLServer\SQLData\ARCHIVE.NDF', SIZE = 51200KB, MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
(NAME = N'PartitionDemo_log', FILENAME = N'C:\SQLServer\SQLLog\PartitionDemo_log.ldf', SIZE = 20480KB, MAXSIZE = 2048GB, FILEGROWTH = 512KB)
GO


/****************************************************************************************
--Create partition function & scheme
*****************************************************************************************/


USE [PartitioningDemo];
GO

CREATE PARTITION FUNCTION PF_PartitionedTable(DATE)
	   AS RANGE RIGHT 
    FOR VALUES ('2013-01-01','2014-01-01','2015-01-01','2016-01-01','2017-01-01');
GO


CREATE PARTITION SCHEME PS_PartitionedTable
    AS PARTITION PF_PartitionedTable
TO ([DATA],[DATA],[DATA],[DATA],[DATA],[DATA]);
GO


/****************************************************************************************
--Create partitioned table
*****************************************************************************************/


CREATE TABLE dbo.PartitionedTable
(PKID INT IDENTITY(1,1),
 ColA VARCHAR(10),
 ColB VARCHAR(10),
 CreatedDate DATE)
 ON PS_PartitionedTable(CreatedDate);


CREATE UNIQUE CLUSTERED INDEX [IX_CreatedDate_PartitionedTable] ON dbo.PartitionedTable
 (CreatedDate,PKID) 
ON PS_PartitionedTable(CreatedDate);
GO


/****************************************************************************************
--Create switch table
*****************************************************************************************/


CREATE TABLE dbo.PartitionedTable_Switch
(PKID INT IDENTITY(1,1),
 ColA VARCHAR(10),
 ColB VARCHAR(10),
 CreatedDate DATE)
 ON [DATA];


CREATE UNIQUE CLUSTERED INDEX [IX_CreatedDate_PartitionedTable_Switch] ON dbo.PartitionedTable_Switch
 (CreatedDate,PKID) 
ON [DATA];
GO


/****************************************************************************************
--Insert data
*****************************************************************************************/

SET NOCOUNT ON;
SET STATISTICS IO OFF;

INSERT INTO dbo.PartitionedTable
(ColA,ColB,CreatedDate)
VALUES
(REPLICATE('A',10),REPLICATE('A',10),'2012-02-01');
GO 10000

INSERT INTO dbo.PartitionedTable
(ColA,ColB,CreatedDate)
VALUES
(REPLICATE('A',10),REPLICATE('A',10),'2013-02-01');
GO 10000

INSERT INTO dbo.PartitionedTable
(ColA,ColB,CreatedDate)
VALUES
(REPLICATE('A',10),REPLICATE('A',10),'2014-02-01');
GO 10000

INSERT INTO dbo.PartitionedTable
(ColA,ColB,CreatedDate)
VALUES
(REPLICATE('A',10),REPLICATE('A',10),'2015-02-01');
GO 10000

INSERT INTO dbo.PartitionedTable
(ColA,ColB,CreatedDate)
VALUES
(REPLICATE('A',10),REPLICATE('A',10),'2016-02-01');
GO 500


/****************************************************************************************
--Check partitions
*****************************************************************************************/


SELECT 
	t.name AS TableName, i.name AS IndexName, p.partition_number, p.partition_id, 
	--i.data_space_id, f.function_id, f.type_desc, fg.name AS [filegroup], 
	r.boundary_id, r.value AS BoundaryValue, p.rows
	--,r.*
FROM 
	sys.tables AS t
INNER JOIN
	sys.indexes AS i ON t.object_id = i.object_id
INNER JOIN
	sys.partitions AS p ON i.object_id = p.object_id AND i.index_id = p.index_id 
INNER JOIN 
    sys.allocation_units a ON a.container_id = p.hobt_id 
INNER JOIN 
    sys.filegroups fg ON fg.data_space_id = a.data_space_id 
INNER JOIN
	sys.partition_schemes AS s ON i.data_space_id = s.data_space_id
INNER JOIN
	sys.partition_functions AS f ON s.function_id = f.function_id
LEFT OUTER JOIN 
	sys.partition_range_values AS r ON f.function_id = r.function_id 
									AND r.boundary_id = p.partition_number
WHERE 
	t.name = 'PartitionedTable'
AND 
	i.type <= 1
AND
    a.type = 1 --in row data only
ORDER BY p.partition_number DESC;
	

/****************************************************************************************
--Split newest partition to create 2018 partition
*****************************************************************************************/


--Tell SQL where the new partiton is going to live
ALTER PARTITION SCHEME PS_PartitionedTable
NEXT USED [DATA];

--Split out the new partition
ALTER PARTITION FUNCTION PF_PartitionedTable()
SPLIT RANGE ('2018-01-01');


/****************************************************************************************
--Check partitions
*****************************************************************************************/


SELECT 
	t.name AS TableName, i.name AS IndexName, p.partition_number, p.partition_id, 
	--i.data_space_id, f.function_id, f.type_desc, fg.name AS [filegroup], 
	r.boundary_id, r.value AS BoundaryValue, p.rows
	--,r.*
FROM 
	sys.tables AS t
INNER JOIN
	sys.indexes AS i ON t.object_id = i.object_id
INNER JOIN
	sys.partitions AS p ON i.object_id = p.object_id AND i.index_id = p.index_id 
INNER JOIN 
    sys.allocation_units a ON a.container_id = p.hobt_id 
INNER JOIN 
    sys.filegroups fg ON fg.data_space_id = a.data_space_id 
INNER JOIN
	sys.partition_schemes AS s ON i.data_space_id = s.data_space_id
INNER JOIN
	sys.partition_functions AS f ON s.function_id = f.function_id
LEFT OUTER JOIN 
	sys.partition_range_values AS r ON f.function_id = r.function_id 
									AND r.boundary_id = p.partition_number
WHERE 
	t.name = 'PartitionedTable'
AND 
	i.type <= 1
AND
    a.type = 1 --in row data only
ORDER BY p.partition_number DESC;



/****************************************************************************************
--Merge oldest partition
*****************************************************************************************/

SET STATISTICS IO ON;

--Merging 2013 partition - There is data present!
ALTER PARTITION FUNCTION PF_PartitionedTable()
MERGE RANGE ('2013-01-01');


/****************************************************************************************
--Check partitions
*****************************************************************************************/


SELECT 
	t.name AS TableName, i.name AS IndexName, p.partition_number, p.partition_id, 
	--i.data_space_id, f.function_id, f.type_desc, fg.name AS [filegroup], 
	r.boundary_id, r.value AS BoundaryValue, p.rows
	--,r.*
FROM 
	sys.tables AS t
INNER JOIN
	sys.indexes AS i ON t.object_id = i.object_id
INNER JOIN
	sys.partitions AS p ON i.object_id = p.object_id AND i.index_id = p.index_id 
INNER JOIN 
    sys.allocation_units a ON a.container_id = p.hobt_id 
INNER JOIN 
    sys.filegroups fg ON fg.data_space_id = a.data_space_id 
INNER JOIN
	sys.partition_schemes AS s ON i.data_space_id = s.data_space_id
INNER JOIN
	sys.partition_functions AS f ON s.function_id = f.function_id
LEFT OUTER JOIN 
	sys.partition_range_values AS r ON f.function_id = r.function_id 
									AND r.boundary_id = p.partition_number
WHERE 
	t.name = 'PartitionedTable'
AND 
	i.type <= 1
AND
    a.type = 1 --in row data only
ORDER BY p.partition_number DESC;



/****************************************************************************************
--Switch partition 1
*****************************************************************************************/


ALTER TABLE [dbo].PartitionedTable
	SWITCH PARTITION 1
TO [dbo].PartitionedTable_Switch;


/****************************************************************************************
--Check partitions
*****************************************************************************************/


SELECT 
	t.name AS TableName, i.name AS IndexName, p.partition_number, p.partition_id, 
	--i.data_space_id, f.function_id, f.type_desc, fg.name AS [filegroup], 
	r.boundary_id, r.value AS BoundaryValue, p.rows
	--,r.*
FROM 
	sys.tables AS t
INNER JOIN
	sys.indexes AS i ON t.object_id = i.object_id
INNER JOIN
	sys.partitions AS p ON i.object_id = p.object_id AND i.index_id = p.index_id 
INNER JOIN 
    sys.allocation_units a ON a.container_id = p.hobt_id 
INNER JOIN 
    sys.filegroups fg ON fg.data_space_id = a.data_space_id 
INNER JOIN
	sys.partition_schemes AS s ON i.data_space_id = s.data_space_id
INNER JOIN
	sys.partition_functions AS f ON s.function_id = f.function_id
LEFT OUTER JOIN 
	sys.partition_range_values AS r ON f.function_id = r.function_id 
									AND r.boundary_id = p.partition_number
WHERE 
	t.name = 'PartitionedTable'
AND 
	i.type <= 1
AND
    a.type = 1 --in row data only
ORDER BY p.partition_number DESC;