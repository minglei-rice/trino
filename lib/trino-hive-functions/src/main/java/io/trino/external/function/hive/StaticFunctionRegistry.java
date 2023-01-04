/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.external.function.hive;

import io.airlift.log.Logger;
import org.apache.hadoop.hive.ql.exec.AmbiguousMethodException;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.NoMatchingMethodException;
import org.apache.hadoop.hive.ql.exec.Registry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFAcos;
import org.apache.hadoop.hive.ql.udf.UDFAscii;
import org.apache.hadoop.hive.ql.udf.UDFAsin;
import org.apache.hadoop.hive.ql.udf.UDFAtan;
import org.apache.hadoop.hive.ql.udf.UDFBase64;
import org.apache.hadoop.hive.ql.udf.UDFBin;
import org.apache.hadoop.hive.ql.udf.UDFChr;
import org.apache.hadoop.hive.ql.udf.UDFConv;
import org.apache.hadoop.hive.ql.udf.UDFCos;
import org.apache.hadoop.hive.ql.udf.UDFCrc32;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorDay;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorHour;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorMinute;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorMonth;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorQuarter;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorSecond;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorWeek;
import org.apache.hadoop.hive.ql.udf.UDFDateFloorYear;
import org.apache.hadoop.hive.ql.udf.UDFDayOfMonth;
import org.apache.hadoop.hive.ql.udf.UDFDayOfWeek;
import org.apache.hadoop.hive.ql.udf.UDFDegrees;
import org.apache.hadoop.hive.ql.udf.UDFE;
import org.apache.hadoop.hive.ql.udf.UDFExp;
import org.apache.hadoop.hive.ql.udf.UDFFindInSet;
import org.apache.hadoop.hive.ql.udf.UDFFromUnixTime;
import org.apache.hadoop.hive.ql.udf.UDFHex;
import org.apache.hadoop.hive.ql.udf.UDFHour;
import org.apache.hadoop.hive.ql.udf.UDFJson;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.UDFLn;
import org.apache.hadoop.hive.ql.udf.UDFLog;
import org.apache.hadoop.hive.ql.udf.UDFLog10;
import org.apache.hadoop.hive.ql.udf.UDFLog2;
import org.apache.hadoop.hive.ql.udf.UDFMd5;
import org.apache.hadoop.hive.ql.udf.UDFMinute;
import org.apache.hadoop.hive.ql.udf.UDFMonth;
import org.apache.hadoop.hive.ql.udf.UDFOPBitAnd;
import org.apache.hadoop.hive.ql.udf.UDFOPBitNot;
import org.apache.hadoop.hive.ql.udf.UDFOPBitOr;
import org.apache.hadoop.hive.ql.udf.UDFOPBitShiftLeft;
import org.apache.hadoop.hive.ql.udf.UDFOPBitShiftRight;
import org.apache.hadoop.hive.ql.udf.UDFOPBitShiftRightUnsigned;
import org.apache.hadoop.hive.ql.udf.UDFOPBitXor;
import org.apache.hadoop.hive.ql.udf.UDFOPLongDivide;
import org.apache.hadoop.hive.ql.udf.UDFPI;
import org.apache.hadoop.hive.ql.udf.UDFParseUrl;
import org.apache.hadoop.hive.ql.udf.UDFRadians;
import org.apache.hadoop.hive.ql.udf.UDFRand;
import org.apache.hadoop.hive.ql.udf.UDFRegExpExtract;
import org.apache.hadoop.hive.ql.udf.UDFRegExpReplace;
import org.apache.hadoop.hive.ql.udf.UDFRepeat;
import org.apache.hadoop.hive.ql.udf.UDFReplace;
import org.apache.hadoop.hive.ql.udf.UDFReverse;
import org.apache.hadoop.hive.ql.udf.UDFSecond;
import org.apache.hadoop.hive.ql.udf.UDFSha1;
import org.apache.hadoop.hive.ql.udf.UDFSign;
import org.apache.hadoop.hive.ql.udf.UDFSin;
import org.apache.hadoop.hive.ql.udf.UDFSpace;
import org.apache.hadoop.hive.ql.udf.UDFSqrt;
import org.apache.hadoop.hive.ql.udf.UDFSubstr;
import org.apache.hadoop.hive.ql.udf.UDFTan;
import org.apache.hadoop.hive.ql.udf.UDFToBoolean;
import org.apache.hadoop.hive.ql.udf.UDFToByte;
import org.apache.hadoop.hive.ql.udf.UDFToDouble;
import org.apache.hadoop.hive.ql.udf.UDFToFloat;
import org.apache.hadoop.hive.ql.udf.UDFToInteger;
import org.apache.hadoop.hive.ql.udf.UDFToLong;
import org.apache.hadoop.hive.ql.udf.UDFToShort;
import org.apache.hadoop.hive.ql.udf.UDFToString;
import org.apache.hadoop.hive.ql.udf.UDFUUID;
import org.apache.hadoop.hive.ql.udf.UDFUnbase64;
import org.apache.hadoop.hive.ql.udf.UDFUnhex;
import org.apache.hadoop.hive.ql.udf.UDFVersion;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.apache.hadoop.hive.ql.udf.UDFYear;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.ql.udf.xml.GenericUDFXPath;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathBoolean;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathDouble;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathFloat;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathInteger;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathLong;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathShort;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathString;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The most codes in this class are copied from {@link FunctionRegistry}.
 *
 * Notice that there are some commented functions, they are in hive 3+, but hive 3+ is
 * not completely compatible with java 11 but java 8, mainly caused by the changes of
 * system classloader hierarchy.
 *
 * Also some UDFs defined in {@link FunctionRegistry} are removed, because they rely
 * on the packages which had been excluded by Trino libs, such as hive-apache.
 */
public class StaticFunctionRegistry
{
    private static final Logger log = Logger.get(StaticFunctionRegistry.class);

    public static final String LEAD_FUNC_NAME = "lead";
    public static final String LAG_FUNC_NAME = "lag";

    public static final String UNARY_PLUS_FUNC_NAME = "positive";
    public static final String UNARY_MINUS_FUNC_NAME = "negative";

    // registry for system functions
    private static final Registry system = new Registry(true);

    static {
        system.registerGenericUDF("concat", GenericUDFConcat.class);
        system.registerUDF("substr", UDFSubstr.class, false);
        system.registerUDF("substring", UDFSubstr.class, false);
        system.registerGenericUDF("substring_index", GenericUDFSubstringIndex.class);
        system.registerUDF("space", UDFSpace.class, false);
        system.registerUDF("repeat", UDFRepeat.class, false);
        system.registerUDF("ascii", UDFAscii.class, false);
        system.registerGenericUDF("lpad", GenericUDFLpad.class);
        system.registerGenericUDF("rpad", GenericUDFRpad.class);
        system.registerGenericUDF("levenshtein", GenericUDFLevenshtein.class);
        system.registerGenericUDF("soundex", GenericUDFSoundex.class);

        system.registerGenericUDF("size", GenericUDFSize.class);

        system.registerGenericUDF("round", GenericUDFRound.class);
        system.registerGenericUDF("bround", GenericUDFBRound.class);
        system.registerGenericUDF("floor", GenericUDFFloor.class);
        system.registerUDF("sqrt", UDFSqrt.class, false);
        system.registerGenericUDF("cbrt", GenericUDFCbrt.class);
        system.registerGenericUDF("ceil", GenericUDFCeil.class);
        system.registerGenericUDF("ceiling", GenericUDFCeil.class);
        system.registerUDF("rand", UDFRand.class, false);
        system.registerGenericUDF("abs", GenericUDFAbs.class);
        system.registerGenericUDF("sq_count_check", GenericUDFSQCountCheck.class);
        // defined in hive 3+, but the dependent hive version is 2+
         system.registerGenericUDF("enforce_constraint", GenericUDFEnforceConstraint.class);
        system.registerGenericUDF("pmod", GenericUDFPosMod.class);

        system.registerUDF("ln", UDFLn.class, false);
        system.registerUDF("log2", UDFLog2.class, false);
        system.registerUDF("sin", UDFSin.class, false);
        system.registerUDF("asin", UDFAsin.class, false);
        system.registerUDF("cos", UDFCos.class, false);
        system.registerUDF("acos", UDFAcos.class, false);
        system.registerUDF("log10", UDFLog10.class, false);
        system.registerUDF("log", UDFLog.class, false);
        system.registerUDF("exp", UDFExp.class, false);
        system.registerGenericUDF("power", GenericUDFPower.class);
        system.registerGenericUDF("pow", GenericUDFPower.class);
        system.registerUDF("sign", UDFSign.class, false);
        system.registerUDF("pi", UDFPI.class, false);
        system.registerUDF("degrees", UDFDegrees.class, false);
        system.registerUDF("radians", UDFRadians.class, false);
        system.registerUDF("atan", UDFAtan.class, false);
        system.registerUDF("tan", UDFTan.class, false);
        system.registerUDF("e", UDFE.class, false);
        system.registerGenericUDF("factorial", GenericUDFFactorial.class);
        system.registerUDF("crc32", UDFCrc32.class, false);

        system.registerUDF("conv", UDFConv.class, false);
        system.registerUDF("bin", UDFBin.class, false);
        system.registerUDF("chr", UDFChr.class, false);
        system.registerUDF("hex", UDFHex.class, false);
        system.registerUDF("unhex", UDFUnhex.class, false);
        system.registerUDF("base64", UDFBase64.class, false);
        system.registerUDF("unbase64", UDFUnbase64.class, false);
        system.registerGenericUDF("sha2", GenericUDFSha2.class);
        system.registerUDF("md5", UDFMd5.class, false);
        system.registerUDF("sha1", UDFSha1.class, false);
        system.registerUDF("sha", UDFSha1.class, false);
        system.registerGenericUDF("aes_encrypt", GenericUDFAesEncrypt.class);
        system.registerGenericUDF("aes_decrypt", GenericUDFAesDecrypt.class);
        system.registerUDF("uuid", UDFUUID.class, false);

        system.registerGenericUDF("encode", GenericUDFEncode.class);
        system.registerGenericUDF("decode", GenericUDFDecode.class);

        system.registerGenericUDF("upper", GenericUDFUpper.class);
        system.registerGenericUDF("lower", GenericUDFLower.class);
        system.registerGenericUDF("ucase", GenericUDFUpper.class);
        system.registerGenericUDF("lcase", GenericUDFLower.class);
        system.registerGenericUDF("trim", GenericUDFTrim.class);
        system.registerGenericUDF("ltrim", GenericUDFLTrim.class);
        system.registerGenericUDF("rtrim", GenericUDFRTrim.class);
        system.registerGenericUDF("length", GenericUDFLength.class);
        system.registerGenericUDF("character_length", GenericUDFCharacterLength.class);
        system.registerGenericUDF("char_length", GenericUDFCharacterLength.class);
        system.registerGenericUDF("octet_length", GenericUDFOctetLength.class);
        system.registerUDF("reverse", UDFReverse.class, false);
        system.registerGenericUDF("field", GenericUDFField.class);
        system.registerUDF("find_in_set", UDFFindInSet.class, false);
        system.registerGenericUDF("initcap", GenericUDFInitCap.class);

        system.registerUDF("like", UDFLike.class, true);
         system.registerGenericUDF("likeany", GenericUDFLikeAny.class);
         system.registerGenericUDF("likeall", GenericUDFLikeAll.class);
        system.registerGenericUDF("rlike", GenericUDFRegExp.class);
        system.registerGenericUDF("regexp", GenericUDFRegExp.class);
        system.registerUDF("regexp_replace", UDFRegExpReplace.class, false);
        system.registerUDF("replace", UDFReplace.class, false);
        system.registerUDF("regexp_extract", UDFRegExpExtract.class, false);
        system.registerUDF("parse_url", UDFParseUrl.class, false);
        system.registerGenericUDF("nvl", GenericUDFNvl.class);
        system.registerGenericUDF("split", GenericUDFSplit.class);
        system.registerGenericUDF("str_to_map", GenericUDFStringToMap.class);
        system.registerGenericUDF("translate", GenericUDFTranslate.class);

        system.registerGenericUDF(UNARY_PLUS_FUNC_NAME, GenericUDFOPPositive.class);
        system.registerGenericUDF(UNARY_MINUS_FUNC_NAME, GenericUDFOPNegative.class);

        system.registerGenericUDF("day", UDFDayOfMonth.class);
        system.registerGenericUDF("dayofmonth", UDFDayOfMonth.class);
        system.registerUDF("dayofweek", UDFDayOfWeek.class, false);
        system.registerGenericUDF("month", UDFMonth.class);
        system.registerGenericUDF("quarter", GenericUDFQuarter.class);
        system.registerGenericUDF("year", UDFYear.class);
        system.registerGenericUDF("hour", UDFHour.class);
        system.registerGenericUDF("minute", UDFMinute.class);
        system.registerGenericUDF("second", UDFSecond.class);
        system.registerUDF("from_unixtime", UDFFromUnixTime.class, false);
        system.registerGenericUDF("to_date", GenericUDFDate.class);
        system.registerUDF("weekofyear", UDFWeekOfYear.class, false);
        system.registerGenericUDF("last_day", GenericUDFLastDay.class);
        system.registerGenericUDF("next_day", GenericUDFNextDay.class);
        system.registerGenericUDF("trunc", GenericUDFTrunc.class);
        system.registerGenericUDF("date_format", GenericUDFDateFormat.class);

        // Special date formatting functions
        system.registerUDF("floor_year", UDFDateFloorYear.class, false);
        system.registerUDF("floor_quarter", UDFDateFloorQuarter.class, false);
        system.registerUDF("floor_month", UDFDateFloorMonth.class, false);
        system.registerUDF("floor_day", UDFDateFloorDay.class, false);
        system.registerUDF("floor_week", UDFDateFloorWeek.class, false);
        system.registerUDF("floor_hour", UDFDateFloorHour.class, false);
        system.registerUDF("floor_minute", UDFDateFloorMinute.class, false);
        system.registerUDF("floor_second", UDFDateFloorSecond.class, false);

        system.registerGenericUDF("date_add", GenericUDFDateAdd.class);
        system.registerGenericUDF("date_sub", GenericUDFDateSub.class);
        system.registerGenericUDF("datediff", GenericUDFDateDiff.class);
        system.registerGenericUDF("add_months", GenericUDFAddMonths.class);
        system.registerGenericUDF("months_between", GenericUDFMonthsBetween.class);

        system.registerUDF("get_json_object", UDFJson.class, false);

        system.registerUDF("xpath_string", UDFXPathString.class, false);
        system.registerUDF("xpath_boolean", UDFXPathBoolean.class, false);
        system.registerUDF("xpath_number", UDFXPathDouble.class, false);
        system.registerUDF("xpath_double", UDFXPathDouble.class, false);
        system.registerUDF("xpath_float", UDFXPathFloat.class, false);
        system.registerUDF("xpath_long", UDFXPathLong.class, false);
        system.registerUDF("xpath_int", UDFXPathInteger.class, false);
        system.registerUDF("xpath_short", UDFXPathShort.class, false);
        system.registerGenericUDF("xpath", GenericUDFXPath.class);

        system.registerUDF("div", UDFOPLongDivide.class, true);

        system.registerUDF("&", UDFOPBitAnd.class, true);
        system.registerUDF("|", UDFOPBitOr.class, true);
        system.registerUDF("^", UDFOPBitXor.class, true);
        system.registerUDF("~", UDFOPBitNot.class, true);
        system.registerUDF("shiftleft", UDFOPBitShiftLeft.class, true);
        system.registerUDF("shiftright", UDFOPBitShiftRight.class, true);
        system.registerUDF("shiftrightunsigned", UDFOPBitShiftRightUnsigned.class, true);

        system.registerGenericUDF("grouping", GenericUDFGrouping.class);

        system.registerGenericUDF("current_database", UDFCurrentDB.class);
        system.registerGenericUDF("current_date", GenericUDFCurrentDate.class);
        system.registerGenericUDF("current_timestamp", GenericUDFCurrentTimestamp.class);
        system.registerGenericUDF("current_user", GenericUDFCurrentUser.class);
        system.registerGenericUDF("current_groups", GenericUDFCurrentGroups.class);
        system.registerGenericUDF("logged_in_user", GenericUDFLoggedInUser.class);
         system.registerGenericUDF("restrict_information_schema", GenericUDFRestrictInformationSchema.class);

        system.registerGenericUDF("isnull", GenericUDFOPNull.class);
        system.registerGenericUDF("isnotnull", GenericUDFOPNotNull.class);
        system.registerGenericUDF("istrue", GenericUDFOPTrue.class);
        system.registerGenericUDF("isnottrue", GenericUDFOPNotTrue.class);
        system.registerGenericUDF("isfalse", GenericUDFOPFalse.class);
        system.registerGenericUDF("isnotfalse", GenericUDFOPNotFalse.class);

        system.registerGenericUDF("between", GenericUDFBetween.class);
        system.registerGenericUDF("in_bloom_filter", GenericUDFInBloomFilter.class);

        // Utility UDFs
        system.registerUDF("version", UDFVersion.class, false);

        // Aliases for Java Class Names
        // These are used in getImplicitConvertUDFMethod
        system.registerUDF(serdeConstants.BOOLEAN_TYPE_NAME, UDFToBoolean.class, false, UDFToBoolean.class.getSimpleName());
        system.registerUDF(serdeConstants.TINYINT_TYPE_NAME, UDFToByte.class, false, UDFToByte.class.getSimpleName());
        system.registerUDF(serdeConstants.SMALLINT_TYPE_NAME, UDFToShort.class, false, UDFToShort.class.getSimpleName());
        system.registerUDF(serdeConstants.INT_TYPE_NAME, UDFToInteger.class, false, UDFToInteger.class.getSimpleName());
        system.registerUDF(serdeConstants.BIGINT_TYPE_NAME, UDFToLong.class, false, UDFToLong.class.getSimpleName());
        system.registerUDF(serdeConstants.FLOAT_TYPE_NAME, UDFToFloat.class, false, UDFToFloat.class.getSimpleName());
        system.registerUDF(serdeConstants.DOUBLE_TYPE_NAME, UDFToDouble.class, false, UDFToDouble.class.getSimpleName());
        system.registerUDF(serdeConstants.STRING_TYPE_NAME, UDFToString.class, false, UDFToString.class.getSimpleName());
        // following mapping is to enable UDFName to UDF while generating expression for default value (in operator tree)
        //  e.g. cast(4 as string) is serialized as UDFToString(4) into metastore, to allow us to generate appropriate UDF for
        //  UDFToString we need the following mappings
        // Rest of the types e.g. DATE, CHAR, VARCHAR etc are already registered
        system.registerUDF(UDFToString.class.getSimpleName(), UDFToString.class, false, UDFToString.class.getSimpleName());
        system.registerUDF(UDFToBoolean.class.getSimpleName(), UDFToBoolean.class, false, UDFToBoolean.class.getSimpleName());
        system.registerUDF(UDFToDouble.class.getSimpleName(), UDFToDouble.class, false, UDFToDouble.class.getSimpleName());
        system.registerUDF(UDFToFloat.class.getSimpleName(), UDFToFloat.class, false, UDFToFloat.class.getSimpleName());
        system.registerUDF(UDFToInteger.class.getSimpleName(), UDFToInteger.class, false, UDFToInteger.class.getSimpleName());
        system.registerUDF(UDFToLong.class.getSimpleName(), UDFToLong.class, false, UDFToLong.class.getSimpleName());
        system.registerUDF(UDFToShort.class.getSimpleName(), UDFToShort.class, false, UDFToShort.class.getSimpleName());
        system.registerUDF(UDFToByte.class.getSimpleName(), UDFToByte.class, false, UDFToByte.class.getSimpleName());

        system.registerGenericUDF(serdeConstants.DATE_TYPE_NAME, GenericUDFToDate.class);
        system.registerGenericUDF(serdeConstants.TIMESTAMP_TYPE_NAME, GenericUDFTimestamp.class);
         system.registerGenericUDF(serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME, GenericUDFToTimestampLocalTZ.class);
        system.registerGenericUDF(serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME, GenericUDFToIntervalYearMonth.class);
        system.registerGenericUDF(serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME, GenericUDFToIntervalDayTime.class);
        system.registerGenericUDF(serdeConstants.BINARY_TYPE_NAME, GenericUDFToBinary.class);
        system.registerGenericUDF(serdeConstants.DECIMAL_TYPE_NAME, GenericUDFToDecimal.class);
        system.registerGenericUDF(serdeConstants.VARCHAR_TYPE_NAME, GenericUDFToVarchar.class);
        system.registerGenericUDF(serdeConstants.CHAR_TYPE_NAME, GenericUDFToChar.class);

        // Generic UDFs
        system.registerGenericUDF("reflect", GenericUDFReflect.class);
        system.registerGenericUDF("reflect2", GenericUDFReflect2.class);
        system.registerGenericUDF("java_method", GenericUDFReflect.class);

        system.registerGenericUDF("array", GenericUDFArray.class);
        system.registerGenericUDF("assert_true", GenericUDFAssertTrue.class);
         system.registerGenericUDF("assert_true_oom", GenericUDFAssertTrueOOM.class);
        system.registerGenericUDF("map", GenericUDFMap.class);
        system.registerGenericUDF("struct", GenericUDFStruct.class);
        system.registerGenericUDF("named_struct", GenericUDFNamedStruct.class);
        system.registerGenericUDF("create_union", GenericUDFUnion.class);
        system.registerGenericUDF("extract_union", GenericUDFExtractUnion.class);

        system.registerGenericUDF("case", GenericUDFCase.class);
        system.registerGenericUDF("when", GenericUDFWhen.class);
        system.registerGenericUDF("nullif", GenericUDFNullif.class);
        system.registerGenericUDF("hash", GenericUDFHash.class);
         system.registerGenericUDF("murmur_hash", GenericUDFMurmurHash.class);
        system.registerGenericUDF("coalesce", GenericUDFCoalesce.class);
        system.registerGenericUDF("index", GenericUDFIndex.class);
        system.registerGenericUDF("in_file", GenericUDFInFile.class);
        system.registerGenericUDF("instr", GenericUDFInstr.class);
        system.registerGenericUDF("locate", GenericUDFLocate.class);
        system.registerGenericUDF("elt", GenericUDFElt.class);
        system.registerGenericUDF("concat_ws", GenericUDFConcatWS.class);
        system.registerGenericUDF("sort_array", GenericUDFSortArray.class);
        system.registerGenericUDF("sort_array_by", GenericUDFSortArrayByField.class);
        system.registerGenericUDF("array_contains", GenericUDFArrayContains.class);
        system.registerGenericUDF("sentences", GenericUDFSentences.class);
        system.registerGenericUDF("map_keys", GenericUDFMapKeys.class);
        system.registerGenericUDF("map_values", GenericUDFMapValues.class);
        system.registerGenericUDF("format_number", GenericUDFFormatNumber.class);
        system.registerGenericUDF("printf", GenericUDFPrintf.class);
        system.registerGenericUDF("greatest", GenericUDFGreatest.class);
        system.registerGenericUDF("least", GenericUDFLeast.class);
        system.registerGenericUDF("cardinality_violation", GenericUDFCardinalityViolation.class);
        system.registerGenericUDF("width_bucket", GenericUDFWidthBucket.class);

        system.registerGenericUDF("from_utc_timestamp", GenericUDFFromUtcTimestamp.class);
        system.registerGenericUDF("to_utc_timestamp", GenericUDFToUtcTimestamp.class);

        system.registerGenericUDF("unix_timestamp", GenericUDFUnixTimeStamp.class);
        system.registerGenericUDF("to_unix_timestamp", GenericUDFToUnixTimeStamp.class);

        system.registerGenericUDF("internal_interval", GenericUDFInternalInterval.class);

        system.registerGenericUDF("to_epoch_milli", GenericUDFEpochMilli.class);

        //PTF declarations
        system.registerGenericUDF(LEAD_FUNC_NAME, GenericUDFLead.class);
        system.registerGenericUDF(LAG_FUNC_NAME, GenericUDFLag.class);

        // Arithmetic specializations are done in a convoluted manner; mark them as built-in.
        system.registerHiddenBuiltIn(GenericUDFOPDTIMinus.class);
        system.registerHiddenBuiltIn(GenericUDFOPDTIPlus.class);
        system.registerHiddenBuiltIn(GenericUDFOPNumericMinus.class);
        system.registerHiddenBuiltIn(GenericUDFOPNumericPlus.class);

        // mask UDFs
        system.registerGenericUDF(GenericUDFMask.UDF_NAME, GenericUDFMask.class);
        system.registerGenericUDF(GenericUDFMaskFirstN.UDF_NAME, GenericUDFMaskFirstN.class);
        system.registerGenericUDF(GenericUDFMaskLastN.UDF_NAME, GenericUDFMaskLastN.class);
        system.registerGenericUDF(GenericUDFMaskShowFirstN.UDF_NAME, GenericUDFMaskShowFirstN.class);
        system.registerGenericUDF(GenericUDFMaskShowLastN.UDF_NAME, GenericUDFMaskShowLastN.class);
        system.registerGenericUDF(GenericUDFMaskHash.UDF_NAME, GenericUDFMaskHash.class);
    }

    /**
     * This method is shared between UDFRegistry and UDAFRegistry. methodName will
     * be "evaluate" for UDFRegistry, and "aggregate"/"evaluate"/"evaluatePartial"
     * for UDAFRegistry.
     * @throws UDFArgumentException
     */
    public static <T> Method getMethodInternal(Class<? extends T> udfClass,
                                               String methodName, boolean exact, List<TypeInfo> argumentClasses)
            throws UDFArgumentException {

        List<Method> mlist = new ArrayList<Method>();

        for (Method m : udfClass.getMethods()) {
            if (m.getName().equals(methodName)) {
                mlist.add(m);
            }
        }

        return getMethodInternal(udfClass, mlist, exact, argumentClasses);
    }

    /**
     * Gets the closest matching method corresponding to the argument list from a
     * list of methods.
     *
     * @param mlist
     *          The list of methods to inspect.
     * @param exact
     *          Boolean to indicate whether this is an exact match or not.
     * @param argumentsPassed
     *          The classes for the argument.
     * @return The matching method.
     */
    public static Method getMethodInternal(Class<?> udfClass, List<Method> mlist, boolean exact,
                                           List<TypeInfo> argumentsPassed) throws UDFArgumentException {

        // result
        List<Method> udfMethods = new ArrayList<Method>();
        // The cost of the result
        int leastConversionCost = Integer.MAX_VALUE;

        for (Method m : mlist) {
            List<TypeInfo> argumentsAccepted = TypeInfoUtils.getParameterTypeInfos(m,
                    argumentsPassed.size());
            if (argumentsAccepted == null) {
                // null means the method does not accept number of arguments passed.
                continue;
            }

            boolean match = (argumentsAccepted.size() == argumentsPassed.size());
            int conversionCost = 0;

            for (int i = 0; i < argumentsPassed.size() && match; i++) {
                int cost = matchCost(argumentsPassed.get(i), argumentsAccepted.get(i),
                        exact);
                if (cost == -1) {
                    match = false;
                } else {
                    conversionCost += cost;
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("Method " + (match ? "did" : "didn't") + " match: passed = "
                        + argumentsPassed + " accepted = " + argumentsAccepted +
                        " method = " + m);
            }
            if (match) {
                // Always choose the function with least implicit conversions.
                if (conversionCost < leastConversionCost) {
                    udfMethods.clear();
                    udfMethods.add(m);
                    leastConversionCost = conversionCost;
                    // Found an exact match
                    if (leastConversionCost == 0) {
                        break;
                    }
                } else if (conversionCost == leastConversionCost) {
                    // Ambiguous call: two methods with the same number of implicit
                    // conversions
                    udfMethods.add(m);
                    // Don't break! We might find a better match later.
                } else {
                    // do nothing if implicitConversions > leastImplicitConversions
                }
            }
        }

        if (udfMethods.size() == 0) {
            // No matching methods found
            throw new NoMatchingMethodException(udfClass, argumentsPassed, mlist);
        }

        if (udfMethods.size() > 1) {
            // First try selecting methods based on the type affinity of the arguments passed
            // to the candidate method arguments.
            filterMethodsByTypeAffinity(udfMethods, argumentsPassed);
        }

        if (udfMethods.size() > 1) {
            // if the only difference is numeric types, pick the method
            // with the smallest overall numeric type.
            int lowestNumericType = Integer.MAX_VALUE;
            boolean multiple = true;
            Method candidate = null;
            List<TypeInfo> referenceArguments = null;

            for (Method m: udfMethods) {
                int maxNumericType = 0;

                List<TypeInfo> argumentsAccepted = TypeInfoUtils.getParameterTypeInfos(m, argumentsPassed.size());

                if (referenceArguments == null) {
                    // keep the arguments for reference - we want all the non-numeric
                    // arguments to be the same
                    referenceArguments = argumentsAccepted;
                }

                Iterator<TypeInfo> referenceIterator = referenceArguments.iterator();

                for (TypeInfo accepted: argumentsAccepted) {
                    TypeInfo reference = referenceIterator.next();

                    boolean acceptedIsPrimitive = false;
                    PrimitiveObjectInspector.PrimitiveCategory acceptedPrimCat = PrimitiveObjectInspector.PrimitiveCategory.UNKNOWN;
                    if (accepted.getCategory() == ObjectInspector.Category.PRIMITIVE) {
                        acceptedIsPrimitive = true;
                        acceptedPrimCat = ((PrimitiveTypeInfo) accepted).getPrimitiveCategory();
                    }
                    if (acceptedIsPrimitive && TypeInfoUtils.numericTypes.containsKey(acceptedPrimCat)) {
                        // We're looking for the udf with the smallest maximum numeric type.
                        int typeValue = TypeInfoUtils.numericTypes.get(acceptedPrimCat);
                        maxNumericType = typeValue > maxNumericType ? typeValue : maxNumericType;
                    } else if (!accepted.equals(reference)) {
                        // There are non-numeric arguments that don't match from one UDF to
                        // another. We give up at this point.
                        throw new AmbiguousMethodException(udfClass, argumentsPassed, mlist);
                    }
                }

                if (lowestNumericType > maxNumericType) {
                    multiple = false;
                    lowestNumericType = maxNumericType;
                    candidate = m;
                } else if (maxNumericType == lowestNumericType) {
                    // multiple udfs with the same max type. Unless we find a lower one
                    // we'll give up.
                    multiple = true;
                }
            }

            if (!multiple) {
                return candidate;
            } else {
                throw new AmbiguousMethodException(udfClass, argumentsPassed, mlist);
            }
        }
        return udfMethods.get(0);
    }

    /**
     * Returns -1 if passed does not match accepted. Otherwise return the cost
     * (usually 0 for no conversion and 1 for conversion).
     */
    public static int matchCost(TypeInfo argumentPassed, TypeInfo argumentAccepted, boolean exact)
    {
        if (argumentAccepted.equals(argumentPassed)
                || TypeInfoUtils.doPrimitiveCategoriesMatch(argumentPassed, argumentAccepted)) {
            // matches
            return 0;
        }
        if (argumentPassed.equals(TypeInfoFactory.voidTypeInfo)) {
            // passing null matches everything
            return 0;
        }
        if (argumentPassed.getCategory().equals(ObjectInspector.Category.LIST)
                && argumentAccepted.getCategory().equals(ObjectInspector.Category.LIST)) {
            // lists are compatible if and only-if the elements are compatible
            TypeInfo argumentPassedElement = ((ListTypeInfo) argumentPassed)
                    .getListElementTypeInfo();
            TypeInfo argumentAcceptedElement = ((ListTypeInfo) argumentAccepted)
                    .getListElementTypeInfo();
            return matchCost(argumentPassedElement, argumentAcceptedElement, exact);
        }
        if (argumentPassed.getCategory().equals(ObjectInspector.Category.MAP)
                && argumentAccepted.getCategory().equals(ObjectInspector.Category.MAP)) {
            // lists are compatible if and only-if the elements are compatible
            TypeInfo argumentPassedKey = ((MapTypeInfo) argumentPassed)
                    .getMapKeyTypeInfo();
            TypeInfo argumentAcceptedKey = ((MapTypeInfo) argumentAccepted)
                    .getMapKeyTypeInfo();
            TypeInfo argumentPassedValue = ((MapTypeInfo) argumentPassed)
                    .getMapValueTypeInfo();
            TypeInfo argumentAcceptedValue = ((MapTypeInfo) argumentAccepted)
                    .getMapValueTypeInfo();
            int cost1 = matchCost(argumentPassedKey, argumentAcceptedKey, exact);
            int cost2 = matchCost(argumentPassedValue, argumentAcceptedValue, exact);
            if (cost1 < 0 || cost2 < 0) {
                return -1;
            }
            return Math.max(cost1, cost2);
        }

        if (argumentAccepted.equals(TypeInfoFactory.unknownTypeInfo)) {
            // accepting Object means accepting everything,
            // but there is a conversion cost.
            return 1;
        }
        if (!exact && TypeInfoUtils.implicitConvertible(argumentPassed, argumentAccepted)) {
            return 1;
        }

        return -1;
    }

    /**
     * Given a set of candidate methods and list of argument types, try to
     * select the best candidate based on how close the passed argument types are
     * to the candidate argument types.
     * For a varchar argument, we would prefer evaluate(string) over evaluate(double).
     * @param udfMethods  list of candidate methods
     * @param argumentsPassed list of argument types to match to the candidate methods
     */
    static void filterMethodsByTypeAffinity(List<Method> udfMethods, List<TypeInfo> argumentsPassed) {
        if (udfMethods.size() > 1) {
            // Prefer methods with a closer signature based on the primitive grouping of each argument.
            // Score each method based on its similarity to the passed argument types.
            int currentScore = 0;
            int bestMatchScore = 0;
            Method bestMatch = null;
            for (Method m: udfMethods) {
                currentScore = 0;
                List<TypeInfo> argumentsAccepted =
                        TypeInfoUtils.getParameterTypeInfos(m, argumentsPassed.size());
                Iterator<TypeInfo> argsPassedIter = argumentsPassed.iterator();
                for (TypeInfo acceptedType : argumentsAccepted) {
                    // Check the affinity of the argument passed in with the accepted argument,
                    // based on the PrimitiveGrouping
                    TypeInfo passedType = argsPassedIter.next();
                    if (acceptedType.getCategory() == ObjectInspector.Category.PRIMITIVE
                            && passedType.getCategory() == ObjectInspector.Category.PRIMITIVE) {
                        PrimitiveObjectInspectorUtils.PrimitiveGrouping acceptedPg = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
                                ((PrimitiveTypeInfo) acceptedType).getPrimitiveCategory());
                        PrimitiveObjectInspectorUtils.PrimitiveGrouping passedPg = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
                                ((PrimitiveTypeInfo) passedType).getPrimitiveCategory());
                        if (acceptedPg == passedPg) {
                            // The passed argument matches somewhat closely with an accepted argument
                            ++currentScore;
                        }
                    }
                }
                // Check if the score for this method is any better relative to others
                if (currentScore > bestMatchScore) {
                    bestMatchScore = currentScore;
                    bestMatch = m;
                } else if (currentScore == bestMatchScore) {
                    bestMatch = null; // no longer a best match if more than one.
                }
            }

            if (bestMatch != null) {
                // Found a best match during this processing, use it.
                udfMethods.clear();
                udfMethods.add(bestMatch);
            }
        }
    }

    public static Object invoke(Method m, Object thisObject, Object... arguments)
            throws HiveException {
        Object o;
        try {
            o = m.invoke(thisObject, arguments);
        } catch (Exception e) {
            StringBuilder argumentString = new StringBuilder();
            if (arguments == null) {
                argumentString.append("null");
            } else {
                argumentString.append("{");
                for (int i = 0; i < arguments.length; i++) {
                    if (i > 0) {
                        argumentString.append(",");
                    }

                    argumentString.append(arguments[i]);
                }
                argumentString.append("}");
            }

            String detailedMsg = e instanceof java.lang.reflect.InvocationTargetException ?
                    e.getCause().getMessage() : e.getMessage();

            throw new HiveException("Unable to execute method " + m + " with arguments "
                    + argumentString + ":" + detailedMsg, e);
        }
        return o;
    }

    public static FunctionInfo getFunctionInfo(String functionName) throws SemanticException
    {
        FunctionInfo info = getTemporaryFunctionInfo(functionName);
        return info != null ? info : system.getFunctionInfo(functionName);
    }

    public static FunctionInfo getTemporaryFunctionInfo(String functionName) throws SemanticException
    {
        Registry registry = SessionState.getRegistry();
        return registry == null ? null : registry.getFunctionInfo(functionName);
    }

    private StaticFunctionRegistry()
    {
        // prevent instantiation
    }
}

