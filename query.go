package postgressrv

type query struct {
    session Session
    sql string
    numCols int
}

// Run the query using the Server's defined queryer
func (q *query) Run() error {
    err := q.session.Query(q)
    if err != nil {
        return q.session.Write(ErrMsg(err))
    } else {
        // TODO: implement different tags
        return q.session.Write(CompleteMsg("SELECT 1"))
    }
}

// See Query
func (q *query) SQL() string {
    return q.sql
}

// See Query
func (q *query) Session() Session {
    return q.session
}

// See Query
func (q *query) WriteColumns(cols ...Column) error {
    if len(cols) < 1 {
        return Errf("Query cannot return 0 columns")
    }

    if q.numCols > 0 {
        return Errf("Cannot call WriteColumns more than once per query")
    }

    q.numCols = len(cols)
    return q.session.Write(RowDescriptionMsg(cols))
}

// See Query
func (q *query) WriteRow(row ...[]byte) error {
    if q.numCols < 1 {
        return Errf("Cannot call WriteRow() before calling WriteColumns()")
    }

    if len(row) != q.numCols {
        return Errf("Mismatching number of columns in row. Want %d ; have %d",
            q.numCols, len(row))
    }

    return q.session.Write(DataRowMsg(row))
}
