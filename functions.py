import psycopg2
from io import StringIO


def copy_from_stringio(conn, df, table, index=True):

    buffer = StringIO()
    df.to_csv(buffer, index=index, header=False,sep=";")
    buffer.seek(0)
    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep=";")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("inserción exitosa")
    cursor.close()
    buffer.close()


def cargar_datos(conn, tabla, df):

    copy_from_stringio(conn, df, tabla, index=False)