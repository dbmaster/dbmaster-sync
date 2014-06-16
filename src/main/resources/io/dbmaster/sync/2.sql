CREATE PROCEDURE [dbo].[usp_pc_get_user_role]
@login varchar(100),
@user_groups varchar(4000),
@store_cd varchar(10) output,
@user_role varchar(16) output
 AS
BEGIN
declare @msg varchar(2000)
set @msg = 'Get role for ' + @login + ' groups '+@user_groups

 RAISERROR (@msg, 0, 1) WITH NOWAIT, LOG
 
 IF UPPER(@login) LIKE 'MOVADOGROUP\\MCS\_%' ESCAPE '\'
 BEGIN 
   set @user_role='store'
   set @store_cd = SUBSTRING(@login, 17,4)
 END ELSE 
  IF UPPER(@login) LIKE 'MOVADOGROUP\\WEBCLOCK%' ESCAPE '\'
  BEGIN 
   set @user_role='store'
   set @store_cd = SUBSTRING(@login, 21,4)
  END ELSE 
  IF  CHARINDEX(UPPER('MOVADOGROUP\US-Retail Piece Counts'),UPPER(@user_groups))>0
  BEGIN 
    set @user_role='home_office'
  END ELSE -- BELOW is only for testing purposes
  IF UPPER(@login) LIKE 'MOVADOGROUP\\INFODATA' ESCAPE '\'
  BEGIN 
   set @user_role='store'
   set @store_cd = '128'
  END ELSE 
  IF UPPER(@login) LIKE 'MOVADOGROUP\\SVDAVICHEN' ESCAPE '\'
  BEGIN 
   set @user_role='home_office'
  END 
  ELSE
   set @user_role = 'unknown'
/*
EXEC sys.xp_readerrorlog 0
sp_cycle_errorlog
declare @user_groups varchar(1200)
set @user_groups ='hello'
EXECute xp_logevent 60000, @user_groups, informational
*/
END