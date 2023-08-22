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

		//コマンドライン上に処理開始のメッセージ
		System.out.println("----------------------------------------");
		
		//コマンドライン引数を請求年月の検索用に編集
		StringBuilder commandLineArg = new StringBuilder(args[0]);
		commandLineArg.insert(4,"-").append("-01").toString();

		//コマンドライン引数を加入者と料金の加入日の絞り込み用にLocalDate型に変換
		LocalDate startJoinDate = LocalDate.parse(commandLineArg);
		LocalDate endJoinDate = LocalDate.parse(commandLineArg).plusMonths(1);

		//加入者情報からSQLでデータ抽出
		List<Map<String,Object>> getMemberList= jdbcTemplate.queryForList("SELECT member_id,mail,name,address,start_date,end_date,payment_method from T_MEMBER WHERE end_date is not null and end_date < ? and start_date >= ?",
		startJoinDate,endJoinDate);
		
		//加入者情報から抽出したデータを格納するListを宣言
		List<Member> memberList = new ArrayList<>();

		//加入者情報から抽出したデータをListに格納
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

		//料金情報から抽出したデータを格納するListを宣言
		List<Charge> chargeList = new ArrayList<>();

		//料金情報からSQLでデータ抽出
		List<Map<String,Object>> getChargeList= jdbcTemplate.queryForList("SELECT charge_id,name,amount,start_date,end_date from T_CHARGE WHERE end_date is not null and end_date < ? and start_date >= ?",
		startJoinDate,endJoinDate);

		//請求データ作成時に請求金額の合計を計算する
		int sumChargeAmount = 0;

		//料金情報から抽出したデータをListに格納
		for(Map<String,Object> map : getChargeList){

			Charge charge = new Charge();

			charge.setChargeId((Integer) map.get("charge_id"));
			charge.setName((String) map.get("name"));
			charge.setAmount((Integer) map.get("amount"));
			charge.setStartDate((LocalDate) map.get("start_date"));
			charge.setEndDate((LocalDate) map.get("end_date"));

			chargeList.add(charge);

			sumChargeAmount += charge.getAmount();
		}

		//請求データ状況の請求年月がバッチの稼働対象か確認
		//コマンドライン上に請求情報確認のメッセージ
		System.out.println(commandLineArg.substring(0, 4) + "年" + commandLineArg.substring(5, 7) + "月分の請求情報を確認しています。");

		//コマンドライン引数に入力された年月が請求データ状況の請求年月に存在するか
		List<Map<String,Object>> getBillingDate 
		= jdbcTemplate.queryForList("SELECT billing_ym FROM T_BILLING_STATUS WHERE billing_ym = ?" , commandLineArg);
		if(getBillingDate.contains(commandLineArg)){

			//請求データ状況の確定の確認
			List<Map<String,Object>> getIsCommited 
			= jdbcTemplate.queryForList("SELECT billing_ym FROM T_BILLING_STATUS WHERE billing_ym = ? AND is_commited = false" , commandLineArg);
			if(getIsCommited.contains("true")){
				
				//請求データ状況の確定が真なのでプログラム終了
				System.exit(0);
			}else{

				//コマンドライン上に未確定請求削除のメッセージ
				System.out.println("データベースから" + commandLineArg.substring(0, 4) + "年" + commandLineArg.substring(4, 6) + "月分の未確定請求情報を削除しています。");

				//請求データ状況の確定が偽なので請求明細データと請求データの対象年月のデータを削除する
				jdbcTemplate.update("DELETE FROM T_BILLING_DETAIL_DATA WHERE  billing_ym = ?" , commandLineArg);

				//コマンドライン上に削除完了のメッセージ
				System.out.println("削除しました。");
			}
		}

		//コマンドライン上に請求ステータス追加のメッセージ
		System.out.println(commandLineArg.substring(0, 4) + "年" + commandLineArg.substring(4, 6) + "月分の請求ステータスを追加しています。");

		//請求データ状況にコマンドライン引数に入力された年月のデータを新規作成する
		int countCreateBillingStatus = jdbcTemplate.update("INSERT INTO T_BILLING_STATUS(billing_ym,is_commited) VALUES (?,false)",commandLineArg);

		//コマンドライン上に請求ステータス追加完了のメッセージ
		System.out.println(countCreateBillingStatus + "件追加しました。");

		//コマンドライン上に請求データ追加のメッセージ
		System.out.println(commandLineArg.substring(0, 4) + "年" + commandLineArg.substring(4, 6) + "月分の請求データを追加しています。");

		//各加入者情報をfor文で取り出す
		for(Member member : memberList){
			//請求データにコマンドライン引数に入力された年月のデータを新規作成する
			jdbcTemplate.update("INSERT INTO T_BILLING_DATA(billing_ym,member_id,mail,name,address,start_date,end_date,payment_method,amount,tax_ratio,total)VALUES(?,?,?,?,?,?,?,?,?,?,?)",
			commandLineArg, member.getMemberId(), member.getMail(),member.getName(),member.getAddress(),member.getJoinedAt(),member.getRetiredAt(),member.getPaymentMethod(),sumChargeAmount,0.1,(sumChargeAmount * 1.1));
		}

		//コマンドライン上に請求データ追加完了のメッセージ
		System.out.println(memberList.size() + "件追加しました。");

		//コマンドライン上に請求明細データ追加のメッセージ
		System.out.println(commandLineArg.substring(0, 4) + "年" + commandLineArg.substring(4, 6) + "月分の請求データを追加しています。");

		//各料金情報をfor文で取り出す
		for(Charge charge : chargeList){
			for(Member member : memberList){
				//請求データにコマンドライン引数に入力された年月のデータを新規作成する
				jdbcTemplate.update("INSERT INTO T_BILLING_DETAIL_DATA(billing_ym,member_id,charge_id,name,amount,start_date,end_date)VALUES(?,?,?,?,?,?,?)",
				commandLineArg,member.getMemberId(),charge.getChargeId(),charge.getName(),charge.getAmount(),charge.getStartDate(),charge.getEndDate());
			}
		}
		//コマンドライン上に請求明細データ追加完了のメッセージ
		System.out.println((memberList.size() * chargeList.size()) + "件追加しました。");

		//コマンドライン上に処理終了のメッセージ
		System.out.println("----------------------------------------");
	}
}	
