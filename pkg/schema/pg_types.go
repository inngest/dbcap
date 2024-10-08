package schema

import "github.com/jackc/pgx/v5/pgtype"

// OIDToTypeName returns the OID to type name, thanks to AI.
func OIDToTypeName(oid uint32) string {
	switch oid {
	case pgtype.BoolOID:
		return "bool"
	case pgtype.ByteaOID:
		return "bytea"
	case pgtype.QCharOID:
		return "char"
	case pgtype.NameOID:
		return "name"
	case pgtype.Int8OID:
		return "int8"
	case pgtype.Int2OID:
		return "int2"
	case pgtype.Int4OID:
		return "int4"
	case pgtype.TextOID:
		return "text"
	case pgtype.OIDOID:
		return "oid"
	case pgtype.TIDOID:
		return "tid"
	case pgtype.XIDOID:
		return "xid"
	case pgtype.CIDOID:
		return "cid"
	case pgtype.JSONOID:
		return "json"
	case pgtype.JSONArrayOID:
		return "json[]"
	case pgtype.PointOID:
		return "point"
	case pgtype.LsegOID:
		return "lseg"
	case pgtype.PathOID:
		return "path"
	case pgtype.BoxOID:
		return "box"
	case pgtype.PolygonOID:
		return "polygon"
	case pgtype.LineOID:
		return "line"
	case pgtype.LineArrayOID:
		return "line[]"
	case pgtype.CIDROID:
		return "cidr"
	case pgtype.CIDRArrayOID:
		return "cidr[]"
	case pgtype.Float4OID:
		return "float4"
	case pgtype.Float8OID:
		return "float8"
	case pgtype.CircleOID:
		return "circle"
	case pgtype.CircleArrayOID:
		return "circle[]"
	case pgtype.UnknownOID:
		return "unknown"
	case pgtype.Macaddr8OID:
		return "macaddr8"
	case pgtype.MacaddrOID:
		return "macaddr"
	case pgtype.InetOID:
		return "inet"
	case pgtype.BoolArrayOID:
		return "bool[]"
	case pgtype.QCharArrayOID:
		return "char[]"
	case pgtype.NameArrayOID:
		return "name[]"
	case pgtype.Int2ArrayOID:
		return "int2[]"
	case pgtype.Int4ArrayOID:
		return "int4[]"
	case pgtype.TextArrayOID:
		return "text[]"
	case pgtype.TIDArrayOID:
		return "tid[]"
	case pgtype.ByteaArrayOID:
		return "bytea[]"
	case pgtype.XIDArrayOID:
		return "xid[]"
	case pgtype.CIDArrayOID:
		return "cid[]"
	case pgtype.BPCharArrayOID:
		return "bpchar[]"
	case pgtype.VarcharArrayOID:
		return "varchar[]"
	case pgtype.Int8ArrayOID:
		return "int8[]"
	case pgtype.PointArrayOID:
		return "point[]"
	case pgtype.LsegArrayOID:
		return "lseg[]"
	case pgtype.PathArrayOID:
		return "path[]"
	case pgtype.BoxArrayOID:
		return "box[]"
	case pgtype.Float4ArrayOID:
		return "float4[]"
	case pgtype.Float8ArrayOID:
		return "float8[]"
	case pgtype.PolygonArrayOID:
		return "polygon[]"
	case pgtype.OIDArrayOID:
		return "oid[]"
	case pgtype.ACLItemOID:
		return "aclitem"
	case pgtype.ACLItemArrayOID:
		return "aclitem[]"
	case pgtype.MacaddrArrayOID:
		return "macaddr[]"
	case pgtype.InetArrayOID:
		return "inet[]"
	case pgtype.BPCharOID:
		return "bpchar"
	case pgtype.VarcharOID:
		return "varchar"
	case pgtype.DateOID:
		return "date"
	case pgtype.TimeOID:
		return "time"
	case pgtype.TimestampOID:
		return "timestamp"
	case pgtype.TimestampArrayOID:
		return "timestamp[]"
	case pgtype.DateArrayOID:
		return "date[]"
	case pgtype.TimeArrayOID:
		return "time[]"
	case pgtype.TimestamptzOID:
		return "timestamptz"
	case pgtype.TimestamptzArrayOID:
		return "timestamptz[]"
	case pgtype.IntervalOID:
		return "interval"
	case pgtype.IntervalArrayOID:
		return "interval[]"
	case pgtype.NumericArrayOID:
		return "numeric[]"
	case pgtype.TimetzOID:
		return "timetz"
	case pgtype.TimetzArrayOID:
		return "timetz[]"
	case pgtype.BitOID:
		return "bit"
	case pgtype.BitArrayOID:
		return "bit[]"
	case pgtype.VarbitOID:
		return "varbit"
	case pgtype.VarbitArrayOID:
		return "varbit[]"
	case pgtype.NumericOID:
		return "numeric"
	case pgtype.RecordOID:
		return "record"
	case pgtype.RecordArrayOID:
		return "record[]"
	case pgtype.UUIDOID:
		return "uuid"
	case pgtype.UUIDArrayOID:
		return "uuid[]"
	case pgtype.JSONBOID:
		return "jsonb"
	case pgtype.JSONBArrayOID:
		return "jsonb[]"
	case pgtype.DaterangeOID:
		return "daterange"
	case pgtype.DaterangeArrayOID:
		return "daterange[]"
	case pgtype.Int4rangeOID:
		return "int4range"
	case pgtype.Int4rangeArrayOID:
		return "int4range[]"
	case pgtype.NumrangeOID:
		return "numrange"
	case pgtype.NumrangeArrayOID:
		return "numrange[]"
	case pgtype.TsrangeOID:
		return "tsrange"
	case pgtype.TsrangeArrayOID:
		return "tsrange[]"
	case pgtype.TstzrangeOID:
		return "tstzrange"
	case pgtype.TstzrangeArrayOID:
		return "tstzrange[]"
	case pgtype.Int8rangeOID:
		return "int8range"
	case pgtype.Int8rangeArrayOID:
		return "int8range[]"
	case pgtype.JSONPathOID:
		return "jsonpath"
	case pgtype.JSONPathArrayOID:
		return "jsonpath[]"
	case pgtype.Int4multirangeOID:
		return "int4multirange"
	case pgtype.NummultirangeOID:
		return "nummultirange"
	case pgtype.TsmultirangeOID:
		return "tsmultirange"
	case pgtype.TstzmultirangeOID:
		return "tstzmultirange"
	case pgtype.DatemultirangeOID:
		return "datemultirange"
	case pgtype.Int8multirangeOID:
		return "int8multirange"
	case pgtype.Int4multirangeArrayOID:
		return "int4multirange[]"
	case pgtype.NummultirangeArrayOID:
		return "nummultirange[]"
	case pgtype.TsmultirangeArrayOID:
		return "tsmultirange[]"
	case pgtype.TstzmultirangeArrayOID:
		return "tstzmultirange[]"
	case pgtype.DatemultirangeArrayOID:
		return "datemultirange[]"
	case pgtype.Int8multirangeArrayOID:
		return "int8multirange[]"
	default:
		return "unknown"
	}
}
