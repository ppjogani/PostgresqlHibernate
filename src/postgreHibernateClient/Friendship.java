package postgreHibernateClient;

import java.io.Serializable;

import javax.persistence.*;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.Index;

@Entity
/*@Cacheable
@Cache(usage=CacheConcurrencyStrategy.TRANSACTIONAL)
*/@Table(name="Friendship")
public class Friendship implements Serializable{

	
	//private FriendshipUserPK userpk = new FriendshipUserPK();
	@Id
	@Index(name="FRIENDSHIP_STATUS")
	private String value;
	
	@Id
	@Index(name="FRIENDSHIP_INVITERID")
	@Column(name = "inviterid", nullable = false)
	private String inviterid;	
	
	@Id
	@Index(name="FRIENDSHIP_INVITEEID")
	@Column(name = "inviteeid", nullable = false)
	private String inviteeid;

	public String getInviterid() {
		return inviterid;
	}
	public void setInviterid(String inviterid) {
		this.inviterid = inviterid;
	}
	public String getInviteeid() {
		return inviteeid;
	}
	public void setInviteeid(String inviteeid) {
		this.inviteeid = inviteeid;
	}

	
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}

}
