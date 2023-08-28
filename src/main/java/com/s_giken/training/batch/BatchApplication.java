package com.s_giken.training.batch;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.transaction.annotation.Transactional;

import com.s_giken.training.batch.model.BatchDate;
import com.s_giken.training.batch.model.Charge;
import com.s_giken.training.batch.model.Member;

@SpringBootApplication
public class BatchApplication implements CommandLineRunner {
	final Logger logger = LoggerFactory.getLogger(BatchApplication.class);
	final private JdbcTemplate jdbcTemplate;

	public BatchApplication(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}
	public static void main(String[] args) {
		SpringApplication.run(BatchApplication.class, args);
	}

	@Override
	public void run(String... args)throws Exception {
		BatchDate batchDate = new BatchDate();
		try {
			batchDate.parseArg(args[0]);
		}catch(Exception e) {
			logger.error("コマンドライン引数の入力値が不正です。");
			return;
		}
		batchExecute(batchDate);
	}

	@Transactional
	public void batchExecute(BatchDate batchDate) {

		List<Member> memberList = new ArrayList<>();
		List<Charge> chargeList = new ArrayList<>();
		
		logger.info("----------------------------------------");

		//加入者情報を抽出する
		memberList = getMemberData(batchDate.getStartTargetDate(), batchDate.getEndTargetDate());

		//料金情報を抽出する
		chargeList = getChargeData(batchDate.getStartTargetDate(), batchDate.getEndTargetDate());
		
		//料金の合計額を計算する
		int sumChargeAmount = addChargeAmount(chargeList);

		//請求データ状況の請求年月がバッチの稼働対象か確認する
		if(isExistBillingData(batchDate) == true){
			return;
		}

		//請求ステータス追加
		addBillingStatus(batchDate);

		//請求データ追加
		addBillingData(memberList, sumChargeAmount, batchDate);

		//請求明細データ追加
		addBillingDeteilData(chargeList, memberList, batchDate);

		logger.info("----------------------------------------");
	}

	//加入者を抽出するメソッド
	public List<Member> getMemberData(LocalDate startJoinDate, LocalDate endJoinDate) {

		List<Member> memberList = new ArrayList<>();

		//加入者情報からSQLでデータ抽出
		SqlRowSet getMemberList= jdbcTemplate.queryForRowSet(
			"SELECT member_id,mail,name,address,start_date,end_date,payment_method "
			+ "from T_MEMBER " 
			+ "WHERE start_date < ? and (end_date >= ? or end_date is null)",
			endJoinDate,
			startJoinDate
		);

		//加入者情報から抽出したデータをListに格納
		while(getMemberList.next()) {
			Member member = new Member();

			member.setMemberId(getMemberList.getInt("member_id"));
			member.setMail(getMemberList.getString("mail"));
			member.setName(getMemberList.getString("name"));
			member.setAddress(getMemberList.getString("address"));
			member.setStartDate(getMemberList.getDate("start_date").toLocalDate());
			try {
				member.setEndDate(getMemberList.getDate("end_date").toLocalDate());
			}catch(java.lang.NullPointerException e) {
				member.setEndDate(null);
			}
			
			member.setPaymentMethod(getMemberList.getInt("payment_method"));

			memberList.add(member);
		}
		return memberList;
	}

	//料金情報を抽出するメソッド
	public List<Charge> getChargeData(LocalDate startJoinDate, LocalDate endJoinDate) {

		List<Charge> chargeList = new ArrayList<Charge>();

		//料金情報からSQLでデータ抽出
		SqlRowSet getChargeList= jdbcTemplate.queryForRowSet(
			"SELECT charge_id,name,amount,start_date,end_date "
			+ "from T_CHARGE " 
			+ "WHERE start_date < ? and (end_date >= ? or end_date is null)",
			endJoinDate,
			startJoinDate
		);

		//料金情報から抽出したデータをListに格納
		while(getChargeList.next()) {

			Charge charge = new Charge();

			charge.setChargeId(getChargeList.getInt("charge_id"));
			charge.setName(getChargeList.getString("name"));
			charge.setAmount(getChargeList.getInt("amount"));
			charge.setStartDate(getChargeList.getDate("start_date").toLocalDate());
			try {
				charge.setEndDate(getChargeList.getDate("end_date").toLocalDate());
			}catch(java.lang.NullPointerException e) {
				charge.setEndDate(null);
			}
			

			chargeList.add(charge);
		}
		return chargeList;
	}

	//請求データ作成時に請求金額の合計を計算するメソッド
	public int addChargeAmount(List<Charge> chargeList) {

		int sumChargeAmount = 0;

		for(Charge charge : chargeList) {
			sumChargeAmount += charge.getAmount();
		}
		return sumChargeAmount;
	}

	//請求データ状況の請求年月がバッチの稼働対象か確認するメソッド
	public boolean isExistBillingData(BatchDate batchDate) {

		logger.info(batchDate.getPrintLogTargetDate() + "分の請求情報を確認しています。");

		//請求データ状況の請求年月にコマンドライン引数に入力された年月の確定済みデータが存在するか
		Integer isExistBillingDate = jdbcTemplate.queryForObject(
			"SELECT COUNT(*) FROM T_BILLING_STATUS WHERE billing_ym = ? AND is_commited = true",
			Integer.class, batchDate.getCommandLineArg()
		);
		 
		if( isExistBillingDate.equals(1)) {
				logger.info("バッチを終了します。");
				return true;
		}else {
			logger.info("データベースから" + batchDate.getPrintLogTargetDate() + "分の未確定請求情報を削除しています。");

			//請求データ状況の確定が偽なので請求データステータスと請求明細データと請求データの対象年月のデータを削除する
			jdbcTemplate.update("DELETE FROM T_BILLING_DETAIL_DATA WHERE billing_ym = ?",
			 batchDate.getCommandLineArg());
			jdbcTemplate.update("DELETE FROM T_BILLING_DATA WHERE billing_ym = ?",
			 batchDate.getCommandLineArg());
			jdbcTemplate.update("DELETE FROM T_BILLING_STATUS WHERE billing_ym = ?",
			 batchDate.getCommandLineArg());

			logger.info("削除しました。");

			return false;
		}
	}

	//請求ステータス追加のメソッド
	public void addBillingStatus(BatchDate batchDate) {

		logger.info(batchDate.getPrintLogTargetDate() + "分の請求ステータスを追加しています。");

		//請求ステータスにコマンドライン引数に入力された年月のデータを新規作成する
		int countCreateBillingStatus = jdbcTemplate.update(
			"INSERT INTO T_BILLING_STATUS(billing_ym,is_commited) VALUES (?,false)" , batchDate.getCommandLineArg());

		logger.info(countCreateBillingStatus + "件追加しました。");
	}

	//請求データ追加のメソッド
	public void addBillingData(List<Member> memberList, int sumChargeAmount, BatchDate batchDate) {
		
		logger.info(batchDate.getPrintLogTargetDate() + "分の請求データを追加しています。");

		//請求データにコマンドライン引数に入力された年月のデータを新規作成する
		for(Member member : memberList) {
			jdbcTemplate.update(
				"INSERT INTO T_BILLING_DATA"
				+ "(billing_ym,member_id,mail,name,address,start_date,end_date,payment_method,amount,tax_ratio,total)" 
				+ "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
				batchDate.getCommandLineArg(),
				member.getMemberId(),
				member.getMail(),
				member.getName(),
				member.getAddress(),
				member.getStartDate(),
				member.getEndDate(),
				member.getPaymentMethod(),
				sumChargeAmount,
				0.1,
				(sumChargeAmount * 1.1)
			);
		}
		logger.info(memberList.size() + "件追加しました。");
	}

	//請求明細データ追加のメソッド
	public void addBillingDeteilData(List<Charge> chargeList, List<Member> memberList, BatchDate batchDate) {
		logger.info(batchDate.getPrintLogTargetDate() + "分の請求明細データを追加しています。");

		//請求明細データにコマンドライン引数に入力された年月のデータを新規作成する
		for(Charge charge : chargeList) {
			for(Member member : memberList) {
				jdbcTemplate.update(
					"INSERT INTO T_BILLING_DETAIL_DATA" 
					+"(billing_ym,member_id,charge_id,name,amount,start_date,end_date)"
					+"VALUES(?,?,?,?,?,?,?)",
					batchDate.getCommandLineArg(),
					member.getMemberId(),
					charge.getChargeId(),
					charge.getName(),
					charge.getAmount(),
					charge.getStartDate(),
					charge.getEndDate()
				);
			}
		}
		logger.info((memberList.size() * chargeList.size()) + "件追加しました。");
	}
}
