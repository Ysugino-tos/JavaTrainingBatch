package com.s_giken.training.batch;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;

import com.s_giken.training.batch.model.Charge;
import com.s_giken.training.batch.model.Member;

@SpringBootApplication
public class BatchApplication implements CommandLineRunner{
	final Logger logger = LoggerFactory.getLogger(BatchApplication.class);
	final private JdbcTemplate jdbcTemplate;

	public BatchApplication(JdbcTemplate jdbcTemplate){
		this.jdbcTemplate = jdbcTemplate;
	}
	public static void main(String[] args) {
		SpringApplication.run(BatchApplication.class, args);
	}

	@Override
	public void run(String... args)throws Exception{

		//T_MEMBERから抽出するデータを格納するハコをList型で作成
		List<Member> memberList = new ArrayList<>();

		//T_MEMBERからSQLでデータ抽出
		List<Map<String,Object>> getMemberList= jdbcTemplate.queryForList("SELECT member_id,mail,name,address,joined_at,retired_at,payment_method from T_MEMBER");

		//T_MEMBERから抽出したデータをListに格納
		for(Map<String,Object> map : getMemberList){

			Member member = new Member();

			member.setMemberId((Integer) map.get("member_id"));
			member.setMail((String) map.get("mail"));
			member.setName((String) map.get("name"));
			member.setAddress((String) map.get("address"));
			member.setJoinedAt((LocalDate) map.get("joined_at"));
			member.setRetiredAt((LocalDate) map.get("retired_at"));
			member.setPaymentMethod((Integer) map.get("payment_method"));

			memberList.add(member);
		}

		//T_Chargeから抽出するデータを格納するハコをList型で作成
		List<Charge> chargeList = new ArrayList<>();

		//T_CHARGEからSQLでデータ抽出
		List<Map<String,Object>> getChargeList= jdbcTemplate.queryForList("SELECT charge_id,name,amount,start_date,end_date from T_CHARGE");

		//T_MEMBERから抽出したデータをListに格納
		for(Map<String,Object> map : getMemberList){

			Charge charge = new Charge();

			charge.setChargeId((Integer) map.get("charge_id"));
			charge.setName((String) map.get("name"));
			charge.setAmount((Integer) map.get("amount"));
			charge.setStartDate((LocalDate) map.get("start_date"));
			charge.setEndDate((LocalDate) map.get("end_date"));

			chargeList.add(charge);
		}
		//請求データ状況の請求年月がバッチの稼働対象か確認
		//コマンドライン引数を請求年月の検索用に編集
		StringBuilder commandLineArg = new StringBuilder(args[0]);
		commandLineArg.insert(4,"-").append("-01").toString();

		//コマンドライン引数に入力された年月が請求データ状況の請求年月に存在するか
		List<Map<String,Object>> getBillingDate 
		= jdbcTemplate.queryForList("SELECT billing_ym FROM T_BILLING_STATUS WHERE billing_ym = ?" , commandLineArg);
		if(getBillingDate.contains(commandLineArg)){

			//請求データ状況の確定の真か偽か
			List<Map<String,Object>> getIsCommited 
			= jdbcTemplate.queryForList("SELECT billing_ym FROM T_BILLING_STATUS WHERE billing_ym = ? AND is_commited = false" , commandLineArg);
			if(getIsCommited.contains("true")){
				
				//請求データ状況の確定が真なのでプログラム終了
				System.exit(0);
			}else{

				//請求データ状況の確定が偽なので請求明細データと請求データの対象年月のデータを削除する
				Integer countDeleteRow = jdbcTemplate.execute("DELETE FROM T_BILLING_DETAIL_DATA WHERE  billing_ym = ?" , commandLineArg);
			}

		}





		
	}
}	
