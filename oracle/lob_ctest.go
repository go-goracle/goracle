/*
Copyright 2013 Tamás Gulácsi

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package oracle

// /* Define PL/SQL statement to be used in program. */
// text *give_raise = (text *) "BEGIN\
//                   RAISE_SALARY(:emp_number,:sal_increase, :new_salary);\
//                      END;";
// OCIBind  *bnd1p = NULL;                      /* the first bind handle */
// OCIBind  *bnd2p = NULL;                     /* the second bind handle */
// OCIBind  *bnd3p = NULL;                      /* the third bind handle */
//
// static void checkerr();
// sb4 status;
//
// main()
// {
//   sword    empno, raise, new_sal;
//   dvoid    *tmp;
//   OCISession *usrhp = (OCISession *)NULL;
// ...
// /* attach to database server, and perform necessary initializations
// and authorizations */
// ...
//       /* allocate a statement handle */
//   checkerr(errhp, OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &stmthp,
//            OCI_HTYPE_STMT, 100, (dvoid **) &tmp));
//
//       /* prepare the statement request, passing the PL/SQL text
//         block as the statement to be prepared */
// checkerr(errhp, OCIStmtPrepare(stmthp, errhp, (text *) give_raise, (ub4)
//       strlen(give_raise), OCI_NTV_SYNTAX, OCI_DEFAULT));
//
//       /* bind each of the placeholders to a program variable */
//  checkerr( errhp, OCIBindByName(stmthp, &bnd1p, errhp, (text *) ":emp_number",
//              -1, (ub1 *) &empno,
//             (sword) sizeof(empno), SQLT_INT, (dvoid *) 0,
//              (ub2 *) 0, (ub2) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT));
//
//  checkerr( errhp, OCIBindByName(stmthp, &bnd2p, errhp, (text *) ":sal_increase",
//              -1, (ub1 *) &raise,
//              (sword) sizeof(raise), SQLT_INT, (dvoid *) 0,
//              (ub2 *) 0, (ub2) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT));
//
//       /* remember that PL/SQL OUT variable are bound, not defined */
//
// checkerr( OCIBindByName(stmthp, &bnd3p, errhp, (text *) ":new_salary",
//              -1, (ub1 *) &new_sal,
//              (sword) sizeof(new_sal), SQLT_INT, (dvoid *) 0,
//              (ub2 *) 0, (ub2) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT));
//
//       /* prompt the user for input values */
// printf("Enter the employee number: ");
// scanf("%d", &empno);
//       /* flush the input buffer */
// myfflush();
//
// printf("Enter employee's raise: ");
// scanf("%d", &raise);
//       /* flush the input buffer */
// myfflush();
//
//   /* execute PL/SQL block*/
//   checkerr(errhp, OCIStmtExecute(svchp, stmthp, errhp, (ub4) 1, (ub4) 0,
//       (OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DEFAULT));
//
//   /* display the new salary, following the raise */
// printf("The new salary is %d\n", new_sal);
// }

/*

#cgo LDFLAGS: -lclntsh

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <oci.h>

sword _lobAlloc(OCIEnv *envhp, dvoid* data, int allocatedElements) {
    int i;
    sword status;
	for (i = 0; i < allocatedElements; i++) {
		if ((status = OCIDescriptorAlloc(envhp,
                (void**)((OCILobLocator**)data + i),
                OCI_DTYPE_LOB, 0, NULL)) != OCI_SUCCESS) {
            return status;
        }
    }
    return status;
}

char *_testLobOut(
    OCIEnv *envhp,
    OCIError *errhp,
	OCISvcCtx *svchp,
    OCIStmt *stmthp,
    text *sql,
    sword *status
)
{
    OCIBind *bindhp = NULL;
    OCILobLocator *clob = NULL;
    //OCIInd ind = OCI_IND_NULL;
    ub4 loblen;

    printf(">>> OCIStmtPrepare2(%p, %p, %p, %s, %d)\n", svchp, &stmthp, errhp,
        sql, (int)strlen((char*)sql));
    if ((*status = OCIStmtPrepare2(svchp, &stmthp, errhp, sql, strlen((char*)sql),
                    0, 0,
                 (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT)) != OCI_SUCCESS) {
        return "Prepare";
    }
    printf("<<< stmthp=%p\n", stmthp);
    printf(">>> OCIDescriptorAlloc(%p, %p)\n", envhp, &clob);
    if (0) {
        if ((*status = OCIDescriptorAlloc((dvoid*)envhp, (dvoid **)&clob,
                         (ub4)OCI_DTYPE_LOB, (size_t)0, (dvoid**)0)) != OCI_SUCCESS) {
            return "Alloc Clob";
        }
    } else {
        if ((*status = _lobAlloc(envhp, (dvoid*)&clob, 1)) != OCI_SUCCESS) {
            return "Alloc CLob";
        }
    }
    printf("<<< clob=%p\n", clob);
    printf(">>> OCIBindByPos(%p, %p, %p, %p)\n", stmthp, &bindhp, errhp, clob);
    if ((*status = OCIBindByPos(stmthp, &bindhp, errhp, 1,
               (void*)(&clob), sizeof(OCILobLocator *),
                 SQLT_CLOB, 0, 0, 0, 0, 0, (ub4) OCI_DEFAULT)) != OCI_SUCCESS) {
        return "Bind";
    }
    printf("<<< bindhp=%p\n", bindhp);
    printf(">>> OCIStmtExecute(%p, %p, %p)\n", svchp, stmthp, errhp);
    if ((*status = OCIStmtExecute(svchp, stmthp, errhp, 1, 0, NULL, NULL, OCI_DEFAULT)) != OCI_SUCCESS) {
        return "Exec";
    }
    printf(">>> OCILobGetLength(%p, %p, %p, %p)\n", svchp, errhp, clob, &loblen);
    if ((*status = OCILobGetLength(svchp, errhp, clob, &loblen)) != OCI_SUCCESS) {
        return "GetLength";
    }
    printf("<<< loblen=%d\n", loblen);
    return "OK";
}
*/
import "C"

import (
	"log"

	//"time"
	"github.com/juju/errgo"
)

func testLobOutC(cur *Cursor, qry string) (err error) {
	qryB := []byte(qry)
	var status C.sword

	msgC := C._testLobOut(cur.environment.handle,
		cur.environment.errorHandle, cur.connection.handle,
		cur.handle, (*C.text)(&qryB[0]), &status)
	msg := C.GoString(msgC)
	log.Printf("status=%d msg=%s", status, msg)

	if err = cur.environment.CheckStatus(status, "C._testLobOut"); err != nil {
		return errgo.Newf("%s: %s", msg, err)
	}
	return nil
}
