// ------------------------------------------------------------------------------
// <auto-generated>
//    Generated by avrogen.exe, version 0.9.0.0
//    Changes to this file may cause incorrect behavior and will be lost if code
//    is regenerated
// </auto-generated>
// ------------------------------------------------------------------------------
namespace com.dv
{
	using System;
	using System.Collections.Generic;
	using System.Text;
	using Avro;
	using Avro.Specific;
	
	public partial class cache_youtube_category_mapping : ISpecificRecord
	{
		public static Schema _SCHEMA = Avro.Schema.Parse(@"{""type"":""record"",""name"":""cache_youtube_category_mapping"",""namespace"":""com.dv"",""fields"":[{""name"":""SEQUENCE"",""type"":""int""},{""name"":""ID"",""type"":""int""},{""name"":""CategoryID"",""type"":[""null"",""int""]},{""name"":""YouTubeCategoryTypeID"",""type"":[""null"",""int""]},{""name"":""CreationTime"",""default"":null,""type"":[""null"",""long""]}]}");
		private int _SEQUENCE;
		private int _ID;
		private System.Nullable<int> _CategoryID;
		private System.Nullable<int> _YouTubeCategoryTypeID;
		private System.Nullable<long> _CreationTime;
		public virtual Schema Schema
		{
			get
			{
				return cache_youtube_category_mapping._SCHEMA;
			}
		}
		public int SEQUENCE
		{
			get
			{
				return this._SEQUENCE;
			}
			set
			{
				this._SEQUENCE = value;
			}
		}
		public int ID
		{
			get
			{
				return this._ID;
			}
			set
			{
				this._ID = value;
			}
		}
		public System.Nullable<int> CategoryID
		{
			get
			{
				return this._CategoryID;
			}
			set
			{
				this._CategoryID = value;
			}
		}
		public System.Nullable<int> YouTubeCategoryTypeID
		{
			get
			{
				return this._YouTubeCategoryTypeID;
			}
			set
			{
				this._YouTubeCategoryTypeID = value;
			}
		}
		public System.Nullable<long> CreationTime
		{
			get
			{
				return this._CreationTime;
			}
			set
			{
				this._CreationTime = value;
			}
		}
		public virtual object Get(int fieldPos)
		{
			switch (fieldPos)
			{
			case 0: return this.SEQUENCE;
			case 1: return this.ID;
			case 2: return this.CategoryID;
			case 3: return this.YouTubeCategoryTypeID;
			case 4: return this.CreationTime;
			default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
			};
		}
		public virtual void Put(int fieldPos, object fieldValue)
		{
			switch (fieldPos)
			{
			case 0: this.SEQUENCE = (System.Int32)fieldValue; break;
			case 1: this.ID = (System.Int32)fieldValue; break;
			case 2: this.CategoryID = (System.Nullable<int>)fieldValue; break;
			case 3: this.YouTubeCategoryTypeID = (System.Nullable<int>)fieldValue; break;
			case 4: this.CreationTime = (System.Nullable<long>)fieldValue; break;
			default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
			};
		}
	}
}
