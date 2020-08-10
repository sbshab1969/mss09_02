package acp.db.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acp.db.utils.*;
import acp.forms.dto.ConstDto;
import acp.utils.*;

public class ConstManagerList extends ManagerList {
  private static Logger logger = LoggerFactory.getLogger(ConstManagerList.class);

  protected List<ConstDto> cacheObj = new ArrayList<>();

  public ConstManagerList() {
    headers = new String[] {"ID"
      , Messages.getString("Column.Name")
      , Messages.getString("Column.Value") 
    };
    types = new Class<?>[] { 
        Long.class
      , String.class
      , String.class
    };
    cntColumns = headers.length;

    fields = new String[] { "mssc_id", "mssc_name", "mssc_value" };
    strFields = StrSqlUtils.buildSelectFields(fields, null);
    
    tableName = "mss_const";
    pkColumn = "mssc_id";
    strAwhere = null;
    seqId = 1000L;

    strFrom = tableName;
    strWhere = strAwhere;
    strOrder = pkColumn;
    // ------------
    prepareQuery(null);
    // ------------
  }

  @Override
  public void prepareQuery(Map<String,String> mapFilter) {
    if (mapFilter != null) {
      setWhere(mapFilter);
    } else {
      strWhere = strAwhere;
    }
    strQuery = StrSqlUtils.buildQuery(strFields, strFrom, strWhere, strOrder);
    strQueryCnt = StrSqlUtils.buildQuery("select count(*) cnt", strFrom, strWhere, null);
  }

  private void setWhere(Map<String,String> mapFilter) {
    // ----------------------------------
    String vName = mapFilter.get("name"); 
    // ----------------------------------
    String phWhere = null;
    String str = null;
    // ---
    if (!StrUtils.emptyString(vName)) {
      str = "upper(mssc_name) like upper('" + vName + "%')";
      phWhere = StrSqlUtils.strAddAnd(phWhere, str);
    }
    strWhere = StrSqlUtils.strAddAnd(strAwhere, phWhere);
  }

  @Override
  public long countRecords() {
    long cntRecords = DbUtils.getValueL(strQueryCnt);
    return cntRecords;    
  }

  @Override
  public List<ConstDto> queryAll() {
    openQueryAll();  // forward
    cacheObj = fetchAll();
    closeQuery();
    return cacheObj;    
  }

  @Override
  public List<ConstDto> fetchPage(int startPos, int cntRows) {
    cacheObj = fetchPart(startPos,cntRows);
    return cacheObj;
  }  
 
  private List<ConstDto> fetchAll() {
    ArrayList<ConstDto> cache = new ArrayList<>();
    try {
      while (rs.next()) {
        ConstDto record = getObject(rs);
        cache.add(record);
      }
    } catch (SQLException e) {
      DialogUtils.errorPrint(e,logger);
      cache = new ArrayList<>();
    }
    return cache;
  }

  private List<ConstDto> fetchPart(int startPos, int cntRows) {
    ArrayList<ConstDto> cache = new ArrayList<>();
    if (startPos <= 0 || cntRows<=0) { 
      return cache;
    }
    try {
      // --------------------------------
      boolean res = rs.absolute(startPos);
      // --------------------------------
      if (res == false) {
        return cache;
      }
      int curRow = 0;
      //------------------------------------------
      do {
        curRow++;
        ConstDto record = getObject(rs);
        cache.add(record);
        if (curRow>=cntRows) break;
        //----------------------------------------
      } while (rs.next());
      //------------------------------------------
    } catch (SQLException e) {
      DialogUtils.errorPrint(e,logger);
      cache = new ArrayList<>();
    }
    return cache;
  }

  private ConstDto getObject(ResultSet rs) throws SQLException {
    //---------------------------------------
    Long rsId = rs.getLong("mssc_id");
    String rsName = rs.getString("mssc_name");
    String rsValue = rs.getString("mssc_value");
    //---------------------------------------
    ConstDto obj = new ConstDto();
    obj.setId(rsId);
    obj.setName(rsName);
    obj.setValue(rsValue);
    //---------------------------------------
    return obj;
  }

}
