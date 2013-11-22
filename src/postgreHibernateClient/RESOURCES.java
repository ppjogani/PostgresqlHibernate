package postgreHibernateClient;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.Index;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import edu.usc.bg.base.ByteIterator;


@Entity
/*@Cacheable
@Cache(usage=CacheConcurrencyStrategy.TRANSACTIONAL)
*/@Table(name="RESOURCES")
public class RESOURCES {
	@Id
	private String rid;

	@Index(name="RESOURCES_CREATORID")
	@Column(name = "creatorid", nullable = false)
	private String creatorid;

	@Index(name="RESOURCES_WALLUSERID")
	@Column(name = "walluserid", nullable = false)
	private String walluserid;
	
	private String type;
	
	private String body;
	
	private String doc;

	@OnDelete(action = OnDeleteAction.CASCADE)
	@OneToMany(mappedBy="rid", orphanRemoval=true)
	private Collection<MANIPULATIONS> resid = new ArrayList<MANIPULATIONS>();

	
/*	@ManyToOne
	@JoinColumn(name="userid")
	private UserEntity user;
*/

	public RESOURCES(){
		
	}
	
	protected String getRid() {
		return rid;
	}

	protected void setRid(String rid) {
		this.rid = rid;
	}

	protected String getCreatorid() {
		return creatorid;
	}

	protected void setCreatorid(String creatorid) {
		this.creatorid = creatorid;
	}

	protected String getWalluserid() {
		return walluserid;
	}

	protected void setWalluserid(String walluserid) {
		this.walluserid = walluserid;
	}

	protected String getType() {
		return type;
	}

	protected void setType(String type) {
		this.type = type;
	}

	protected String getBody() {
		return body;
	}

	protected void setBody(String body) {
		this.body = body;
	}

	protected String getDoc() {
		return doc;
	}

	protected void setDoc(String doc) {
		this.doc = doc;
	}

}
