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
#cgo CFLAGS: -I/usr/include/oracle/11.2/client64
#cgo LDFLAGS: -lclntsh -L/usr/lib/oracle/11.2/client64/lib

#include <stdlib.h>
#include <string.h>
#include <oci.h>

sword _testLobOut(
    OCIEnv *envhp,
    OCIError *errhp,
	OCISvcCtx *svchp,
    OCIStmt *stmthp,
    text *sql
)
{
    OCIBind *bindhp = NULL;
    OCILobLocator *clob = NULL;
    ub4 loblen;
    sword status;

    if ((status = OCIDescriptorAlloc((dvoid*)envhp, (dvoid **)&clob,
                     (ub4)OCI_DTYPE_LOB, (size_t)0, (dvoid**)0)) != OCI_SUCCESS) {
        return status;
    }
    if ((status = OCIStmtPrepare(stmthp, errhp, sql, strlen((char*)sql),
                 (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT)) != OCI_SUCCESS) {
        return status;
    }
    if ((status = OCIBindByPos(stmthp, &bindhp, errhp, 1, (dvoid *)clob, 8000,
                 SQLT_CLOB, 0, 0, 0, 0, 0, (ub4) OCI_DEFAULT)) != OCI_SUCCESS) {
        return status;
    }
    if ((status = OCIStmtExecute(svchp, stmthp, errhp, 1, 0, 0, 0, OCI_DEFAULT)) != OCI_SUCCESS) {
        return status;
    }
    if ((status = OCILobGetLength(svchp, errhp, clob, &loblen)) != OCI_SUCCESS) {
        return status;
    }
    return status;
}
*/
import "C"

import (
//"fmt"
//"log"
//"time"
)

func testLobOutC(cur *Cursor, qry string) (err error) {
	qryB := []byte(qry)

	if err = cur.environment.CheckStatus(
		C._testLobOut(cur.environment.handle,
			cur.environment.errorHandle, cur.connection.handle,
			cur.handle, (*C.text)(&qryB[0])),
		"C._testLobOut"); err != nil {
		return err
	}
	return nil
}
