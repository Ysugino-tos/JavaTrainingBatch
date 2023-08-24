package com.s_giken.training.batch;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cglib.core.Local;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.transaction.annotation.Transactional;

import com.s_giken.training.batch.model.BatchDate;
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

		BatchDate batchDate = new BatchDate();

		batchDate = commandLineArgsBuilder(args[0]);

		batchExecute(batchDate);
	}


	//コマンドライン引数を請求年月の検索用に編集するメソッド
	public BatchDate commandLineArgsBuilder(String args){
		BatchDate batchDate = new BatchDate();
		StringBuilder commandLineArg = new StringBuilder(args);
		commandLineArg.insert(4,"-").append("-01").toString();
		LocalDate startJoinDate = LocalDate.parse(commandLineArg);
		LocalDate endJoinDate = LocalDate.parse(commandLineArg).plusMonths(1);
		
		batchDate.setCommandLineArg(commandLineArg);
		batchDate.setStartJoinDate(startJoinDate);
		batchDate.setEndJoinDate(endJoinDate);

		return batchDate;
	}


	@Transactional
	public void batchExecute(BatchDate batchDate){
		List<Member> memberList = new ArrayList<>();
		List<Charge> chargeList = new ArrayList<>();
		
		logger.info("----------------------------------------");

		memberList = getMemberData(batchDate.getStartJoinDate(), batchDate.getEndJoinDate());

		chargeList = getChargeData(batchDate.getStartJoinDate(), batchDate.getEndJoinDate());

		int sumChargeAmount = addChargeAmount(chargeList);

		//請求データ状況の請求年月がバッチの稼働対象か確認するメソッド
		isExistBillingData(batchDate.getCommandLineArg());

		//請求ステータス追加のメソッドの呼び出し
		addBillingStatus(batchDate.getCommandLineArg());

		//請求データ追加のメソッド呼び出し
		addBillingData(memberList, sumChargeAmount, batchDate.getCommandLineArg());

		//請求明細データ追加のメソッドの呼び出し
		addBillingDeteilData(chargeList, memberList, batchDate.getCommandLineArg());

		//コマンドライン上に処理終了のメッセージ
		logger.info("----------------------------------------");
	}

	//加入者を抽出するメソッド
	public List<Member> getMemberData(LocalDate startJoinDate, LocalDate endJoinDate){

		List<Member> memberList = new ArrayList<>();

		//加入者情報からSQLでデータ抽出
		SqlRowSet getMemberList= jdbcTemplate.queryForRowSet("SELECT member_id,mail,name,address,start_date,end_date,payment_method from T_MEMBER WHERE start_date < ? and (end_date >= ? or end_date is null)",
		endJoinDate,startJoinDate);

		//加入者情報から抽出したデータをListに格納
		while(getMemberList.next()){
			Member member = new Member();

			member.setMemberId(getMemberList.getInt("member_id"));
			member.setMail(getMemberList.getString("mail"));
			member.setName(getMemberList.getString("name"));
			member.setAddress(getMemberList.getString("address"));
			member.setStartDate(getMemberList.getDate("start_date").toLocalDate());
			member.setEndDate(getMemberList.getDate("end_date").toLocalDate());
			member.setPaymentMethod(getMemberList.getInt("payment_method"));

			memberList.add(member);
		}
		return memberList;
	}


	//料金情報を抽出するメソッド
	public List<Charge> getChargeData(LocalDate startJoinDate, LocalDate endJoinDate){

		List<Charge> chargeList = new ArrayList<Charge>();

		//料金情報からSQLでデータ抽出
		SqlRowSet getChargeList= jdbcTemplate.queryForRowSet("SELECT charge_id,name,amount,start_date,end_date from T_CHARGE WHERE start_date < ? and (end_date >= ? or end_date is null)",
		endJoinDate,startJoinDate);

		//料金情報から抽出したデータをListに格納
		while(getChargeList.next()){

			Charge charge = new Charge();

			charge.setChargeId(getChargeList.getInt("charge_id"));
			charge.setName(getChargeList.getString("name"));
			charge.setAmount(getChargeList.getInt("amount"));
			charge.setStartDate(getChargeList.getDate("start_date").toLocalDate());
			charge.setEndDate(getChargeList.getDate("end_date").toLocalDate());

			chargeList.add(charge);
		}
		return chargeList;
	}


	//請求データ作成時に請求金額の合計を計算するメソッド
	public int addChargeAmount(List<Charge> chargeList){

		int sumChargeAmount = 0;

		for(Charge charge : chargeList){
			sumChargeAmount += charge.getAmount();
		}
		return sumChargeAmount;
	}


	//請求データ状況の請求年月がバッチの稼働対象か確認するメソッド
	public void isExistBillingData(StringBuilder commandLineArg){
		logger.info(commandLineArg.substring(0, 4) + "年" + commandLineArg.substring(5, 7) + "月分の請求情報を確認しています。");

		//請求データ状況の請求年月にコマンドライン引数に入力された年月の確定済みデータが存在するか
		Integer isExistBillingDate
		= jdbcTemplate.queryForObject("SELECT COUNT(*) FROM T_BILLING_STATUS WHERE billing_ym = ? AND is_commited = true", Integer.class, commandLineArg);
		if( isExistBillingDate.equals(1)){

				//請求データ状況の確定が真なのでプログラム終了
				logger.info("バッチを終了します。");
				System.exit(0);
		}else{

			//コマンドライン上に未確定請求削除のメッセージ
			logger.info("データベースから" + commandLineArg.substring(0, 4) + "年" + commandLineArg.substring(5, 7) + "月分の未確定請求情報を削除しています。");

			//請求データ状況の確定が偽なので請求データステータスと請求明細データと請求データの対象年月のデータを削除する
			jdbcTemplate.update("DELETE FROM T_BILLING_DETAIL_DATA WHERE billing_ym = ?" , commandLineArg);
			jdbcTemplate.update("DELETE FROM T_BILLING_DATA WHERE billing_ym = ?" , commandLineArg);
			jdbcTemplate.update("DELETE FROM T_BILLING_STATUS WHERE billing_ym = ?" , commandLineArg);

			//コマンドライン上に削除完了のメッセージ
			logger.info("削除しました。");	
		}
	}


	//請求ステータス追加のメソッド
	public void addBillingStatus(StringBuilder commandLineArg){

		//コマンドライン上に請求ステータス追加のメッセージ
		logger.info(commandLineArg.substring(0, 4) + "年" + commandLineArg.substring(5, 7) + "月分の請求ステータスを追加しています。");

		//請求ステータスにコマンドライン引数に入力された年月のデータを新規作成する
		int countCreateBillingStatus = jdbcTemplate.update("INSERT INTO T_BILLING_STATUS(billing_ym,is_commited) VALUES (?,false)" , commandLineArg);

		//コマンドライン上に請求ステータス追加完了のメッセージ
		logger.info(countCreateBillingStatus + "件追加しました。");
	}


	//請求データ追加のメソッド
	public void addBillingData(List<Member> memberList, int sumChargeAmount, StringBuilder commandLineArg){
		//コマンドライン上に請求データ追加のメッセージ
		logger.info(commandLineArg.substring(0, 4) + "年" + commandLineArg.substring(5, 7) + "月分の請求データを追加しています。");

		//各加入者情報をfor文で取り出す
		for(Member member : memberList){
			//請求データにコマンドライン引数に入力された年月のデータを新規作成する
			jdbcTemplate.update("INSERT INTO T_BILLING_DATA(billing_ym,member_id,mail,name,address,start_date,end_date,payment_method,amount,tax_ratio,total)VALUES(?,?,?,?,?,?,?,?,?,?,?)",
			commandLineArg, member.getMemberId(), member.getMail(),member.getName(),member.getAddress(),member.getStartDate(),member.getEndDate(),member.getPaymentMethod(),sumChargeAmount,0.1,(sumChargeAmount * 1.1));
		}
		//コマンドライン上に請求データ追加完了のメッセージ
		logger.info(memberList.size() + "件追加しました。");
	}


	//請求明細データ追加のメソッド
	public void addBillingDeteilData(List<Charge> chargeList, List<Member> memberList, StringBuilder commandLineArg){
		//コマンドライン上に請求明細データ追加のメッセージ
		logger.info(commandLineArg.substring(0, 4) + "年" + commandLineArg.substring(5, 7) + "月分の請求明細データを追加しています。");

		//各料金情報をfor文で取り出す
		for(Charge charge : chargeList){
			for(Member member : memberList){
				//請求明細データにコマンドライン引数に入力された年月のデータを新規作成する
				jdbcTemplate.update("INSERT INTO T_BILLING_DETAIL_DATA(billing_ym,member_id,charge_id,name,amount,start_date,end_date)VALUES(?,?,?,?,?,?,?)",
				commandLineArg,member.getMemberId(),charge.getChargeId(),charge.getName(),charge.getAmount(),charge.getStartDate(),charge.getEndDate());
			}
		}
		//コマンドライン上に請求明細データ追加完了のメッセージ
		logger.info((memberList.size() * chargeList.size()) + "件追加しました。");
	}
}
