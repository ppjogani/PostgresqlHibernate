package postgreHibernateClient;

import java.io.Serializable;
import java.sql.Blob;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import javax.persistence.Cacheable;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinColumns;
import javax.persistence.JoinTable;
import javax.persistence.Lob;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.NotFound;
import org.hibernate.annotations.NotFoundAction;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import edu.usc.bg.base.ByteIterator;



@Entity
/*@Cacheable
@Cache(usage=CacheConcurrencyStrategy.TRANSACTIONAL)
*/@Table(name="USERS")
public class USERS implements Serializable{
	@Id
	private String userid;
	
	private String username;
	
	private String pw;
	
	private String fname;
	
	private String lname;
	
	private String gender;
	
	private String dob;
	
	private String jdate;
	
	private String ldate;
	
	private String address;
	
	private String email;
	
	private String tel;
	
	@NotFound(action=NotFoundAction.IGNORE)
	private byte[] pic;
	
	@NotFound(action=NotFoundAction.IGNORE)
	private byte[] tpic;
	
	private int confFriendCnt =0;
	
	private int pendFriendCnt=0;
	
	private int resCnt=0;
	
	


	/*    @ManyToMany(cascade=CascadeType.ALL)
	@JoinColumns({
		@JoinColumn(name="inviteeid", referencedColumnName="userid"),
		@JoinColumn(name="inviterid", referencedColumnName="userid")
	})
*/ 
	@OnDelete(action = OnDeleteAction.CASCADE)
	@OneToMany(mappedBy="inviterid", orphanRemoval=true)
	private Collection<Friendship> friend1 = new ArrayList<Friendship>();
	
	@OnDelete(action = OnDeleteAction.CASCADE)
	@OneToMany(mappedBy="inviteeid", orphanRemoval=true)
	private Collection<Friendship> friend2 = new ArrayList<Friendship>();

	@OnDelete(action = OnDeleteAction.CASCADE)
	@OneToMany(mappedBy="creatorid", orphanRemoval=true)
	private Collection<RESOURCES> friend3 = new ArrayList<RESOURCES>();
	
	@OnDelete(action = OnDeleteAction.CASCADE)
	@OneToMany(mappedBy="walluserid", orphanRemoval=true)
	private Collection<RESOURCES> friend4 = new ArrayList<RESOURCES>();

	
	@OnDelete(action = OnDeleteAction.CASCADE)
	@OneToMany(mappedBy="creatorid", orphanRemoval=true)
	private Collection<MANIPULATIONS> friend5 = new ArrayList<MANIPULATIONS>();
	
	@OnDelete(action = OnDeleteAction.CASCADE)
	@OneToMany(mappedBy="modifierid", orphanRemoval=true)
	private Collection<MANIPULATIONS> friend6 = new ArrayList<MANIPULATIONS>();

	
	public USERS(){
	}

	public int getConfFriendCnt() {
		return confFriendCnt;
	}


	public void setConfFriendCnt(int confFriendCnt) {
		this.confFriendCnt = confFriendCnt;
	}


	public int getPendFriendCnt() {
		return pendFriendCnt;
	}


	public void setPendFriendCnt(int pendFriendCnt) {
		this.pendFriendCnt = pendFriendCnt;
	}


	public int getResCnt() {
		return resCnt;
	}


	public void setResCnt(int resCnt) {
		this.resCnt = resCnt;
	}
	
	public Collection<Friendship> getFriend1() {
		return friend1;
	}

	public void setFriend1(Collection<Friendship> friend1) {
		this.friend1 = friend1;
	}

	protected String getUserid() {
		return userid;
	}

	protected void setUserid(String userid) {
		this.userid = userid;
	}

	protected String getUsername() {
		return username;
	}

	protected void setUsername(String username) {
		this.username = username;
	}

	protected String getPw() {
		return pw;
	}

	protected void setPw(String pw) {
		this.pw = pw;
	}

	protected String getFname() {
		return fname;
	}

	protected void setFname(String fname) {
		this.fname = fname;
	}

	protected String getLname() {
		return lname;
	}

	protected void setLname(String lname) {
		this.lname = lname;
	}

	protected String getGender() {
		return gender;
	}

	protected void setGender(String gender) {
		this.gender = gender;
	}

	protected String getDob() {
		return dob;
	}

	protected void setDob(String dob) {
		this.dob = dob;
	}

	protected String getJdate() {
		return jdate;
	}

	protected void setJdate(String jdate) {
		this.jdate = jdate;
	}

	protected String getLdate() {
		return ldate;
	}

	protected void setLdate(String ldate) {
		this.ldate = ldate;
	}

	protected String getAddress() {
		return address;
	}

	protected void setAddress(String address) {
		this.address = address;
	}

	protected String getEmail() {
		return email;
	}

	protected void setEmail(String email) {
		this.email = email;
	}

	protected String getTel() {
		return tel;
	}

	protected void setTel(String tel) {
		this.tel = tel;
	}


	public byte[] getPic() {
		return pic;
	}


	public void setPic(byte[] pic) {
		this.pic = pic;
	}


	public byte[] getTpic() {
		return tpic;
	}


	public void setTpic(byte[] tpic) {
		this.tpic = tpic;
	}

	
	

}
