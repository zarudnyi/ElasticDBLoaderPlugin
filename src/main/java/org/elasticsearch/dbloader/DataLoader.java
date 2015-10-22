package org.elasticsearch.dbloader;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Ivan Zarudnyi on 11.10.2015.
 */
public class DataLoader {
    private String query = "SELECT `tbl`.*, `tbl`.mbr_id as member_mbr_id, COUNT(DISTINCT `notes`.`note_id`) `mbr_notes_count` , `mtypes`.`mbrtype_name` AS `mbr_type_name`, `wkp`.`wkp_id` AS `akf_workplaces_wkp_id`, `wkp`.`wkp_emp_id` AS `akf_workplaces_wkp_emp_id` , `wkp`.`wkp_status` AS `akf_workplaces_wkp_status` , `wkp`.`wkp_name` AS `akf_workplaces_wkp_name` , `wkp`.`wkp_municipality_number` AS `akf_workplaces_wkp_municipality_number` , `wkp`.`wkp_account_number` AS `akf_workplaces_wkp_account_number` , `wkp`.`wkp_org_number` AS `akf_workplaces_wkp_org_number` , `wkp`.`wkp_phone` AS `akf_workplaces_wkp_phone` , `wkp`.`wkp_mobile` AS `akf_workplaces_wkp_mobile` , `wkp`.`wkp_email` AS `akf_workplaces_wkp_email` , `wkp`.`wkp_address` AS `akf_workplaces_wkp_address` , `wkp`.`wkp_post_code` AS `akf_workplaces_wkp_post_code` , `wkp_city`.`city_name` AS `akf_workplaces_wkp_city` , `emp`.`emp_id` AS `akf_employers_emp_id` , `emp`.`emp_name` AS `akf_employers_emp_name` , `emp`.`emp_status` AS `akf_employers_emp_status` , `emp`.`emp_reg_number` AS `akf_employers_emp_reg_number` , `emp`.`emp_account_number` AS `akf_employers_emp_account_number` , `emp`.`emp_org_number` AS `akf_employers_emp_org_number` , `emp`.`emp_phone` AS `akf_employers_emp_phone` , `emp`.`emp_mobile` AS `akf_employers_emp_mobile` , `emp`.`emp_email` AS `akf_employers_emp_email` , `emp`.`emp_address` AS `akf_employers_emp_address` , `emp`.`emp_post_code` AS `akf_employers_emp_post_code` , `emp_city`.`city_name` AS `akf_employers_emp_city` , `mtypes`.`mbrtype_name` AS `akf_membertypes_mbrtype_name` , `mbrpmt`.`pmtmet_name` AS `akf_paymentmethods_pmtmet_name` , `mbrpmtper`.`pmtper_name` AS `akf_paymentperiods_pmtper_name`, CONCAT_WS(' ', tbl.mbr_firstname, tbl.mbr_surname) as mbr_name, IF(`tbl`.`mbr_abroad`, `tbl`.`mbr_city`, `city`.`city_name`) as city_name, IF(`tbl`.`mbr_abroad`, `tbl`.`mbr_municipality`, `kom`.`kom_name`) as kom_name, IF(`tbl`.`mbr_abroad`, `tbl`.`mbr_county`, `fyl`.`fyl_name`) as fyl_name, concat('[',group_concat(DISTINCT '{\"members2role_id\": \"', roles.members2role_id,    '\", \"role_id\": \"', roles.role_id,    '\", \"role_status\": \"', roles.role_status,    '\", \"role_name\": \"', r.role_name,    '\", \"role_mbr_id\": \"', roles.role_mbr_id,    '\", \"role_target_id\": \"', roles.role_target_id,    '\", \"role_target_type\": \"', roles.role_target_type,    '\", \"role_start\": \"', roles.role_start,    '\", \"role_end\": \"', roles.role_end,    '\"}'),    ']') as roles_list, concat('[',group_concat(DISTINCT '{\"grp_id\": \"', groups.grp_id,    '\", \"grp_name\": \"', groups.grp_name,    '\", \"grp_status\": \"', groups.grp_status,    '\", \"grp_type\": \"', groups.grp_type,    '\", \"grp_created\": \"', groups.grp_created,    '\"}'),    ']') as groups_list, concat('[',group_concat(DISTINCT '{\"field_id\": \"', course_fields.field_id,    '\", \"field_course_id\": \"', course_fields.field_course_id,    '\", \"field_name\": \"', course_fields.field_name,    '\", \"field_label\": \"', course_fields.field_label,    '\", \"field_type\": \"', course_fields.field_type,    '\", \"field_mandatory\": \"', course_fields.field_mandatory,    '\", \"field_product_id\": \"', course_fields.field_product_id,    '\"}'),    ']') as course_fields, concat('[',group_concat(DISTINCT '{\"course_user_id\": \"', cu.course_user_id,    '\", \"course_user_course_id\": \"', cu.course_user_course_id,    '\", \"course_user_mbr_id\": \"', cu.course_user_mbr_id,    '\", \"course_user_registration_date\": \"', cu.course_user_registration_date,    '\", \"course_user_status\": \"', cu.course_user_status,    '\", \"course_user_unreg_reason\": \"', cu.course_user_unreg_reason,    '\"}'),    ']') as course_users, concat('[',group_concat(DISTINCT '{\"payment_product_id\": \"', course_payments.payment_product_id,    '\", \"payment_product_name\": \"', escape_character(course_payments.payment_product_name),    '\", \"payment_product_price\": \"', course_payments.payment_product_price,    '\", \"payment_product_membership\": \"', course_payments.payment_product_membership,    '\", \"payment_product_account\": \"', course_payments.payment_product_account,    '\", \"payment_product_division\": \"', course_payments.payment_product_division,    '\", \"payment_product_used\": \"', course_payments.payment_product_used,    '\",  \"payment_product_type\": \"', course_payments.payment_product_type,    '\"}'),    ']') as course_field_products ,(select payment_bill_status from wghlm_akf_payment_bill where payment_bill_type='K' and payment_bill_target_id = `tbl`.`mbr_id` order by payment_bill_period_start desc limit 1) as akf_payment_bill FROM  `wghlm_akf_members` AS `tbl`  LEFT JOIN `wghlm_akf_notes` AS `notes` ON (`notes`.`note_related_id`=`tbl`.`mbr_id` AND `notes`.`note_type` = 'M') LEFT JOIN `wghlm_akf_workplaces` AS `wkp` ON (`wkp`.`wkp_id` = `tbl`.`mbr_wkp_id`) LEFT JOIN `wghlm_akf_employers` AS `emp` ON (`emp`.`emp_id` = `wkp`.`wkp_emp_id`) LEFT JOIN `wghlm_akf_membertypes` AS `mtypes` ON (`mtypes`.`mbrtype_id` = `tbl`.`mbr_type`) LEFT JOIN `wghlm_akf_paymentmethods` AS `mbrpmt` ON (`mbrpmt`.`pmtmet_id` = `tbl`.`mbr_pmtmet_id`) LEFT JOIN `wghlm_akf_paymentperiods` AS `mbrpmtper` ON (`mbrpmtper`.`pmtper_id` = `tbl`.`mbr_pmtper_id`) LEFT JOIN `wghlm_akf_city` AS `city` ON `city`.`city_post` = `tbl`.`mbr_post_code` LEFT JOIN `wghlm_akf_city` AS `wkp_city` ON `wkp_city`.`city_post` = `wkp`.`wkp_post_code` LEFT JOIN `wghlm_akf_city` AS `emp_city` ON `emp_city`.`city_post` = `emp`.`emp_post_code` LEFT JOIN `wghlm_akf_kommune` AS `kom` ON `kom`.`kom_id` = `city`.`city_kom_id` LEFT JOIN `wghlm_akf_fylke` AS `fyl` ON `fyl`.`fyl_id` = `kom`.`kom_fyl_id` LEFT JOIN `wghlm_akf_members2roles` AS `roles` ON (`roles`.`role_mbr_id` = `tbl`.`mbr_id`) LEFT JOIN `wghlm_akf_groups` AS `groups` ON (`roles`.`role_target_type` = 'G' AND `roles`.`role_status` = 'A' AND `groups`.`grp_id` = `roles`.`role_target_id`) LEFT JOIN `wghlm_akf_course_user` AS `cu` ON (`cu`.`course_user_mbr_id` = `tbl`.`mbr_id`) LEFT JOIN `wghlm_akf_course` AS `courses` ON (`courses`.`course_id` = `cu`.`course_user_course_id`) LEFT JOIN `wghlm_akf_course_field` AS `course_fields` ON (`course_fields`.`field_course_id` = `cu`.`course_user_course_id`) LEFT JOIN `wghlm_akf_course_field_data` AS `course_field_data` ON (`course_field_data`.`field_value_mbr_id` = `tbl`.`mbr_id` AND `course_field_data`.`field_value_field_id` = `course_fields`.`field_id`) LEFT JOIN `wghlm_akf_payment_product` AS `course_payments` ON (`course_payments`.`payment_product_id` = `course_fields`.`field_product_id`) LEFT JOIN `wghlm_akf_payment_bill` AS `payments` ON (`payments`.`payment_bill_target_id` = `tbl`.`mbr_id` AND `payments`.`payment_bill_type` = 'K') LEFT JOIN `wghlm_akf_roles` AS `r` ON (`roles`.`role_id` = r.`role_id`)  GROUP BY `tbl`.`mbr_id` ";
    private ConfigProvider config;

    public DataLoader(ConfigProvider cfg) {
        config  = cfg;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://"+config.getHost()+":"+config.getPort()+"/choo_api?zeroDateTimeBehavior=convertToNull",config.getUser(), config.getPassword());
    }
    private ResultSet fetchData(Connection conn, String index,JSONArray ids) throws SQLException {
        String query = config.getQuery(index);
        if (ids.length()>0){
            query = query.replace("%WHERE%" , config.getQueryWhere(index));
            query = query.replace("%IDS%", ids.join(","));
        } else {
            query = query.replace("%WHERE%" ,"");
        }

        PreparedStatement preparedStatement = conn.prepareStatement(query);

        return preparedStatement.executeQuery();
    }

    public void indexData(BulkRequest bulkRequest, String index, String type, JSONArray ids) throws Exception {
        Connection conn = getConnection();

        ResultSet rs = fetchData(conn, index,ids);

        while (rs.next()) {
            IndexRequest indexRequest = new IndexRequest(index, type, "" + rs.getInt(1));
            JSONObject data = parseRow(rs);
            indexRequest.source(data.toString());
            bulkRequest.add(indexRequest);
        }
        conn.close();

    }

    public void updateData(BulkRequest bulkRequest, String index, String type, JSONArray ids) throws Exception {
        Connection conn = getConnection();

        ResultSet rs = fetchData(conn, index,ids);

        while (rs.next()) {
            UpdateRequest indexRequest = new UpdateRequest(index, type, "" + rs.getInt(1));
            JSONObject data = parseRow(rs);
            indexRequest.doc(data.toString());
            bulkRequest.add(indexRequest);
        }
        conn.close();

    }

    private JSONObject parseRow(ResultSet rs) throws SQLException {
        JSONObject o = new JSONObject();
        String label;
        String data;
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            label = metaData.getColumnLabel(i);
            data = rs.getString(i);
            if (data!=null && data.length()>0 && data.charAt(0)=='[')
                o.put(label, new JSONArray(data));
            else if (data!=null && data.length()>0 && data.charAt(0)=='{')
                o.put(label, new JSONObject(data));
            else
                o.put(label, data);
        }
        return o;
    }

}
