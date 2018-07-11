CREATE OR REPLACE PROCEDURE XXUD_BULK_USER_CREATION_PROC IS
--DECLARE
V_MESSAGE           VARCHAR2(200);
V_STATUS            VARCHAR2(1);
V_ERROR_MSG         VARCHAR2(300);
V_VALID_MSG         VARCHAR2(800);
V_MANAGER_NAME      VARCHAR2(300);
V_CODE_BLOCK        VARCHAR2(100);
V_FIRST_NAME        XXUD_USER_CREATION_BULK_DETAIL.FIRST_NAME%TYPE;
V_LAST_NAME         XXUD_USER_CREATION_BULK_DETAIL.LAST_NAME%TYPE;
V_PROFILE_ID        NUMBER;
V_CIRCLE_ID         NUMBER;
V_CC_ID             NUMBER;
V_COUNT             NUMBER;
V_ADMINID           NUMBER;
V_USERTYPE_ID       NUMBER;
V_MANAGER_ID        NUMBER;
V_FAIL              NUMBER;
V_SUCCESS           NUMBER;
V_TOTAL             NUMBER;
V_CIRCLE_ID_CC      NUMBER;
BEGIN
 FOR J IN (SELECT * FROM XXUD_USER_CREATION_BULK_STATUS  WHERE STATUS='INITIATED') LOOP
    --PROCESSING
    UPDATE XXUD_USER_CREATION_BULK_STATUS
    SET STATUS='PROCESSING'
        ,UPDATED_DATE=SYSDATE
    WHERE UPLOAD_ID=J.UPLOAD_ID;
    COMMIT;    
        FOR I IN (SELECT * FROM XXUD_USER_CREATION_BULK_DETAIL WHERE STATUS IS NULL AND UPLOAD_ID=J.UPLOAD_ID) LOOP
            --INITIALIZATION
            V_MESSAGE:=NULL;
            V_STATUS:='T';
            V_ERROR_MSG:=NULL;
            V_VALID_MSG:=NULL;
            V_PROFILE_ID:=NULL;
            V_CIRCLE_ID:=NULL;
            V_CIRCLE_ID_CC:=NULL;
            V_CC_ID:=NULL;
            V_MANAGER_NAME:=NULL;
            --MANDATORY FIELDS
            IF I.OLM_ID IS NULL THEN
                V_MESSAGE:='OLM_ID,';
            END IF;
            IF I.FIRST_NAME IS NULL THEN
                V_MESSAGE:=V_MESSAGE||'FIRST_NAME,';  
            END IF;
            IF I.PROFILE_NAME IS NULL THEN
                V_MESSAGE:=V_MESSAGE||'PROFILE_NAME,';
            END IF;
            IF I.CIRCLE_NAME IS NULL THEN
                V_MESSAGE:=V_MESSAGE||'CIRCLE_NAME,';
            END IF;
            IF I.CCNAME  IS NULL THEN
                V_MESSAGE:=V_MESSAGE||'CCNAME,';
            END IF;   
            IF I.APPLICATION_ACCESSIBILITY IS NULL THEN
                V_MESSAGE:=V_MESSAGE||'APPLICATION_ACCESSIBILITY,';
            END IF;
            IF I.USER_RIGHT IS NULL THEN
                V_MESSAGE:=V_MESSAGE||'USER_RIGHT,';
            END IF;    
            IF I.USER_GROUP IS NULL THEN
                V_MESSAGE:=V_MESSAGE||'USER_GROUP,';
            END IF;  
            IF I.USER_TYPE IS NULL THEN
                V_MESSAGE:=V_MESSAGE||'USER_TYPE,';
            END IF;
            IF I.SR_NUMBER IS NULL THEN
                V_MESSAGE:=V_MESSAGE||'SR_NUMBER,';
            END IF;
            IF V_MESSAGE IS NOT NULL THEN
                V_STATUS:='F';
                V_ERROR_MSG:=SUBSTR(V_MESSAGE,1,LENGTH(V_MESSAGE)-1/* <-TO ERASE COMMA IN THE LAST ERROR*/)||' IS REQUIRED';
            END IF;
            --MANAGER BOTH DETAILS SHOULD BE PRESENT
            IF (I.MANAGER_NAME IS NULL AND I.MANAGER_OLM_ID IS NOT NULL) OR
               (I.MANAGER_NAME IS NOT NULL AND I.MANAGER_OLM_ID IS NULL ) THEN
                V_STATUS:='F';
                SELECT NVL2(V_ERROR_MSG,V_ERROR_MSG||',',V_ERROR_MSG)||'MANAGER NAME AND OLM ID BOTH SHOULD BE PROVIDED' INTO V_ERROR_MSG FROM DUAL;   
            END IF;
            --LENGTH VALIDATION
            IF V_STATUS='T' THEN
                IF LENGTH(I.FIRST_NAME)> 100 THEN
                    V_MESSAGE:=V_MESSAGE||'FIRST_NAME,';
                END IF;
                IF LENGTH(I.LAST_NAME)> 100 THEN
                    V_MESSAGE:=V_MESSAGE||'LAST_NAME,';
                END IF;
                IF LENGTH(I.EMAIL_ID)> 50 THEN
                    V_MESSAGE:=V_MESSAGE||'EMAIL_ID,';
                END IF;
                IF LENGTH(I.PHONENO)> 50 THEN
                    V_MESSAGE:=V_MESSAGE||'PHONENO,';
                END IF;
                IF LENGTH(I.SR_NUMBER)> 10 THEN
                    V_MESSAGE:=V_MESSAGE||'SR_NUMBER,';
                END IF;
                IF LENGTH(I.REMARKS)> 60 THEN
                    V_MESSAGE:=V_MESSAGE||'REMARKS,';
                END IF;
                IF V_MESSAGE IS NOT NULL THEN
                V_STATUS:='F';
                V_ERROR_MSG:=SUBSTR(V_MESSAGE,1,LENGTH(V_MESSAGE)-1)||' EXCEEDED LENGTH LIMIT';
                END IF;
            END IF;
            --ID VALIDATION
            IF V_STATUS='T' THEN
                SELECT COUNT(1) INTO V_COUNT 
                FROM XXUD_TBLUSER WHERE UPPER(LOGINNAME)=UPPER(I.OLM_ID) AND ISDELETED=0;
                IF V_COUNT!=0 THEN
                    V_STATUS:='F';
                    V_ERROR_MSG:='OLM ID ALREADY EXISTS';     
                END IF;
            END IF; 
            --DUPLICATE OLM ID    
            IF V_STATUS='T' THEN
                SELECT COUNT(1) INTO V_COUNT 
                FROM XXUD_USER_CREATION_BULK_DETAIL 
                WHERE UPPER(OLM_ID)=UPPER(I.OLM_ID) AND UPLOAD_ID=I.UPLOAD_ID ;
                IF V_COUNT>1 THEN
                    V_STATUS:='F';
                    V_ERROR_MSG:='DUPLICATE OLM ID';     
                END IF;
            END IF;     
            
            --VALIDATING THE DATA
            IF V_STATUS='T' THEN        
                --PROFILE_NAME
                BEGIN
                    SELECT PROFILE_ID INTO V_PROFILE_ID
                    FROM XXUD_TBLPROFILE
                    WHERE UPPER(PROFILENAME)=UPPER(I.PROFILE_NAME) AND ISDELETED=0;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        V_STATUS:='F';
                        V_VALID_MSG:='INVALID PROFILE NAME,';
                    WHEN TOO_MANY_ROWS  THEN
                        V_STATUS:='F';
                        V_VALID_MSG:='MANY ROWS FETCHED FOR PROFILE NAME,';                    
                    WHEN OTHERS THEN
                        V_STATUS:='F';
                        V_VALID_MSG:='UNABLE TO FETCH PROFILE NAME,';
                END;
                --CIRCLE
                BEGIN
                    SELECT CIRCLE_ID INTO V_CIRCLE_ID
                    FROM XXUD_TBLCIRCLE
                    WHERE UPPER(CIRCLENAME)=UPPER(I.CIRCLE_NAME) AND ISDELETED=0;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        V_STATUS:='F';
                        V_VALID_MSG:=V_VALID_MSG||'INVALID CIRCLE NAME,';
                    WHEN TOO_MANY_ROWS  THEN
                        V_STATUS:='F';
                        V_VALID_MSG:=V_VALID_MSG||'MANY ROWS FETCHED FOR CIRCLE NAME,';
                    WHEN OTHERS THEN
                        V_STATUS:='F';
                        V_VALID_MSG:=V_VALID_MSG||'UNABLE TO FETCH CIRCLE NAME,';
                END;
                --CALL CENTER
                BEGIN
                    SELECT CIRCLE_ID,CC_ID INTO V_CIRCLE_ID_CC,V_CC_ID 
                    FROM XXUD_TBLCALLCENTER
                    WHERE UPPER(CCNAME)=UPPER(I.CCNAME) AND ISDELETED=0;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        V_STATUS:='F';
                        V_VALID_MSG:=V_VALID_MSG||'INVALID CALL CENTER NAME,';
                    WHEN TOO_MANY_ROWS  THEN
                        V_STATUS:='F';
                        V_VALID_MSG:=V_VALID_MSG||'MANY ROWS FETCHED FOR CALL CENTER NAME,';
                    WHEN OTHERS THEN
                        V_STATUS:='F';
                        V_VALID_MSG:=V_VALID_MSG||'UNABLE TO FETCH CALL CENTER NAME,';
                END;
                --CIRCLE AND CALL CENTER MAPPING
                IF V_CIRCLE_ID_CC!=V_CIRCLE_ID AND V_CIRCLE_ID_CC IS NOT NULL AND V_CIRCLE_ID IS NOT NULL THEN
                    V_STATUS:='F';
                    V_VALID_MSG:=V_VALID_MSG||'CALL CENTER DOESNOT EXIST IN THE CIRCLE PROVIDED,';
                END IF;
                --APPLICATION ACCESSIBILITY 
                IF UPPER(I.APPLICATION_ACCESSIBILITY) NOT IN ('UNIFIED DESKTOP','SINGLE SCREEN') THEN
                    V_STATUS:='F';
                    V_VALID_MSG:=V_VALID_MSG||'INVALID APPLICATION ACCESSIBILITY,';
                END IF;
                --USER GROUP
                IF UPPER(I.USER_GROUP) NOT IN ('IBM','AIRTEL') THEN
                    V_STATUS:='F';
                    V_VALID_MSG:=V_VALID_MSG||'INVALID USER GROUP,';
                END IF;                
               --USER RIGHTS
                BEGIN
                    SELECT ADMINID INTO V_ADMINID
                    FROM XXUD_TBLADMINTYPE
                    WHERE UPPER(ADMINROLENAME)=UPPER(I.USER_RIGHT) AND ISDELETED=0;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        V_STATUS:='F';
                        V_VALID_MSG:=V_VALID_MSG||'INVALID USER RIGHT,';
                    WHEN TOO_MANY_ROWS  THEN
                        V_STATUS:='F';
                        V_VALID_MSG:=V_VALID_MSG||'MANY ROWS FETCHED FOR USER RIGHT,';
                    WHEN OTHERS THEN
                        V_STATUS:='F';
                        V_VALID_MSG:=V_VALID_MSG||'UNABLE TO FETCH USER RIGHT,';
                END;
               --USER TYPE
                BEGIN
                    SELECT USERTYPE_ID INTO V_USERTYPE_ID
                    FROM XXUD_TBLUSERTYPE
                    WHERE UPPER(USERTYPENAME)=UPPER(I.USER_TYPE) AND ISDELETED=0;
                    IF V_USERTYPE_ID=3 OR (V_USERTYPE_ID=2 AND I.MANAGER_OLM_ID IS NOT NULL) THEN
                        IF I.MANAGER_NAME IS NULL AND I.MANAGER_OLM_ID IS NULL THEN
                            V_STATUS:='F';
                            V_VALID_MSG:=V_VALID_MSG||'MANAGER DETAILS ARE MANDATORY FOR CSR USER TYPE,';
                        ELSIF I.MANAGER_NAME IS NULL AND I.MANAGER_OLM_ID IS NOT NULL THEN
                            V_STATUS:='F';
                            V_VALID_MSG:=V_VALID_MSG||'MANAGER NAME IS MANDATORY FOR CSR USER TYPE,';
                        ELSIF I.MANAGER_NAME IS NOT NULL AND I.MANAGER_OLM_ID IS NULL THEN
                            V_STATUS:='F';
                            V_VALID_MSG:=V_VALID_MSG||'MANAGER OLD ID IS MANDATORY FOR CSR USER TYPE,';
                        ELSE
                            --MANAGER NAME AND MANAGER ID AND CALL CENTER MAPPING
                            BEGIN
                                SELECT UPPER(FIRSTNAME||' '||LASTNAME),USER_ID INTO V_MANAGER_NAME,V_MANAGER_ID
                                FROM XXUD_TBLUSER
                                WHERE USERTYPE_ID IN (1,2)  
                                AND ENABLED = 1 AND  ISDELETED=0 
                                AND UPPER(LOGINNAME)=UPPER(I.MANAGER_OLM_ID);
                                IF UPPER(I.MANAGER_NAME)!=V_MANAGER_NAME THEN
                                     V_STATUS:='F';
                                     V_VALID_MSG:=V_VALID_MSG||'INVALID MANAGER NAME,';
                                ELSE
                                    SELECT COUNT(1) INTO V_COUNT
                                    FROM XXUD_TBLUSER
                                    WHERE CC_ID=V_CC_ID
                                    AND UPPER(LOGINNAME)=UPPER(I.MANAGER_OLM_ID);
                                    IF V_COUNT=0 THEN
                                        V_STATUS:='F';
                                        V_VALID_MSG:=V_VALID_MSG||'MANAGER NOT MAPPED TO CALL CENTER PROVIDED'; 
                                    END IF;
                                END IF;
                            EXCEPTION
                                WHEN NO_DATA_FOUND THEN
                                    V_STATUS:='F';
                                    V_VALID_MSG:=V_VALID_MSG||'INVALID MANAGER OLM ID,';
                                WHEN TOO_MANY_ROWS  THEN
                                    V_STATUS:='F';
                                    V_VALID_MSG:=V_VALID_MSG||'MANY ROWS FETCHED FOR MANAGER OLM ID,';
                                WHEN OTHERS THEN
                                    V_STATUS:='F';
                                    V_VALID_MSG:=V_VALID_MSG||'UNABLE TO FETCH MANAGER OLM ID,';
                            END;
                        END IF;
                    END IF;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        V_STATUS:='F';
                        V_VALID_MSG:=V_VALID_MSG||'INVALID USER TYPE,';
                    WHEN TOO_MANY_ROWS  THEN
                        V_STATUS:='F';
                        V_VALID_MSG:=V_VALID_MSG||'MANY ROWS FETCHED FOR USER TYPE,';
                    WHEN OTHERS THEN
                        V_STATUS:='F';
                        V_VALID_MSG:=V_VALID_MSG||'UNABLE TO FETCH USER TYPE,';
                END;                                  
            END IF; 
            --UPDATING THE STATUS IF ERRORED                     
            IF V_STATUS='F' THEN       
                UPDATE XXUD_USER_CREATION_BULK_DETAIL
                  SET STATUS=V_STATUS
                     ,ERROR_MSG=V_ERROR_MSG||SUBSTR(V_VALID_MSG,1,LENGTH(V_VALID_MSG)-1)
                     ,UPDATED_DATE=SYSDATE
                WHERE REQUEST_ID=I.REQUEST_ID;
                COMMIT;
            ELSE
                BEGIN
                    --REMOVING SPECIAL CHARACTER
                    V_CODE_BLOCK:='WHILE REMOVING SPECIAL CHARATER';
                    SELECT TRIM(TRANSLATE (I.FIRST_NAME,'0`~!@#$%^&*()-_=+[]{}\|;:''",<.>/?' ,'0')) INTO V_FIRST_NAME FROM DUAL;
                    SELECT TRIM(TRANSLATE (I.LAST_NAME,'0`~!@#$%^&*()-_=+[]{}\|;:''",<.>/?' ,'0')) INTO V_LAST_NAME FROM DUAL;
                    V_CODE_BLOCK:='WHILE INSERTING IN XXUD_TBLUSER';
                    INSERT INTO XXUD_TBLUSER(USER_ID,LOGINNAME,FIRSTNAME,LASTNAME,ENABLED
                                            ,PROFILE_ID,CC_ID,MANAGERID,ISADMIN,USERTYPE_ID
                                            ,EMAILID,PHONENO,CREATED_DATE,USER_GROUP
                                            ,ISSSUSER,CREATED_BY)
                                      VALUES(XXUD_USERID_SEQ.NEXTVAL,UPPER(I.OLM_ID),V_FIRST_NAME,V_LAST_NAME,1
                                            ,V_PROFILE_ID,V_CC_ID,NVL(V_MANAGER_ID,0),V_ADMINID,V_USERTYPE_ID
                                            ,I.EMAIL_ID,I.PHONENO,SYSDATE,UPPER(I.USER_GROUP)
                                            ,DECODE(UPPER(I.APPLICATION_ACCESSIBILITY),'UNIFIED DESKTOP',0,'SINGLE SCREEN',1),-99
                                            );
                    V_CODE_BLOCK:='WHILE INSERTING IN XXUD_TBLUSERSRREQ';
                    INSERT INTO XXUD_TBLUSERSRREQ(TBLUSERSRREQ_ID,SR_NO,REMARK1,CREATEDON,USER_ID
                                                 ,AFFECTED_UID,AFFECTED_USER_NAME)
                                          VALUES (XXUD_TBLUSERSRREQ_SEQ.NEXTVAL,I.SR_NUMBER,I.REMARKS,SYSDATE,I.CREATED_BY                               ,XXUD_USERID_SEQ.CURRVAL,I.OLM_ID );
                      COMMIT;
                    UPDATE XXUD_USER_CREATION_BULK_DETAIL
                      SET STATUS='S'
                         ,UPDATED_DATE=SYSDATE
                    WHERE REQUEST_ID=I.REQUEST_ID;
                    COMMIT;
                EXCEPTION
                    WHEN OTHERS THEN
                        ROLLBACK;
                        V_ERROR_MSG := V_CODE_BLOCK||'-'||SQLCODE||':'||SUBSTR(SQLERRM, 1, 200);
                        UPDATE XXUD_USER_CREATION_BULK_DETAIL
                          SET STATUS='F'
                             ,ERROR_MSG=V_ERROR_MSG
                             ,UPDATED_DATE=SYSDATE
                        WHERE REQUEST_ID=I.REQUEST_ID;
                        COMMIT;                          
                END;
            END IF; 
        END LOOP;
        SELECT COUNT(1) INTO V_TOTAL FROM XXUD_USER_CREATION_BULK_DETAIL WHERE UPLOAD_ID=J.UPLOAD_ID;
        SELECT COUNT(1) INTO V_SUCCESS FROM XXUD_USER_CREATION_BULK_DETAIL WHERE UPLOAD_ID=J.UPLOAD_ID AND STATUS='S';
        SELECT COUNT(1) INTO V_FAIL FROM XXUD_USER_CREATION_BULK_DETAIL WHERE UPLOAD_ID=J.UPLOAD_ID AND STATUS='F';
        --COMPLETED
        UPDATE XXUD_USER_CREATION_BULK_STATUS
        SET STATUS='COMPLETED'
           ,UPDATED_DATE=SYSDATE
           ,TOTAL_RECORDS=V_TOTAL
           ,SUCCESS_RECORDS=V_SUCCESS
           ,FAILED_RECORDS=V_FAIL
        WHERE UPLOAD_ID=J.UPLOAD_ID;
        COMMIT; 
  END LOOP; 
END;
