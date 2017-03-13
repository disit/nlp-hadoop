/* Nlp-Hadoop
   Copyright (C) 2017 DISIT Lab http://www.disit.org - University of Florence

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as
   published by the Free Software Foundation, either version 3 of the
   License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>. */
   

package principale;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBInputWritable implements Writable, DBWritable{
    
    
    private int id;
    private String url;

    public void readFields(DataInput in) throws IOException{}

    public void readFields(ResultSet rs) throws SQLException{
    //Resultset object represents the data returned from a SQL statement
        id = rs.getInt(1);
        url = rs.getString(2);
    }

    public void write(DataOutput out) throws IOException{}

    public void write(PreparedStatement ps) throws SQLException{
        
        ps.setInt(1, id);
        ps.setString(2, url);
    }

    public int getId(){
       return id;
    }

    public String getUrl(){
       return url;
    }
}